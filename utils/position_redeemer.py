"""
Position Redeemer Module

This module provides functionality to redeem traded positions back to USDC
when a market closes/resolves, ensuring funds are available for the next market.

Uses redeemPositions (for resolved markets with unequal positions) and 
mergePositions (for equal YES/NO positions before resolution) as needed.
"""

import os
import logging
import requests
from typing import Optional, List, Tuple
from web3 import Web3
from eth_account import Account
from dotenv import load_dotenv
from web3.middleware import ExtraDataToPOAMiddleware
from abi.ctfAbi import ctf_abi
from abi.safeAbi import safe_abi

load_dotenv()

logger = logging.getLogger(__name__)

# Constants
CONDITIONAL_TOKENS_FRAMEWORK_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER_ADDRESS = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
USDC_ADDRESS = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
USDCE_DIGITS = 6


def get_redeemable_positions(market_slug: Optional[str] = None) -> List[dict]:
    """
    Fetch redeemable positions for the user, optionally filtered by market slug.
    
    Args:
        market_slug: Optional market slug to filter positions
        
    Returns:
        List of redeemable position dictionaries
    """
    try:
        proxy_address = os.getenv('POLYMARKET_PROXY_ADDRESS')
        
        # Fetch all positions (including both mergeable and non-mergeable)
        all_positions = []
        
        # First, fetch mergeable positions (balanced Yes/No)
        mergeable_url = f"https://data-api.polymarket.com/positions?sizeThreshold=1&limit=100&sortBy=TOKENS&sortDirection=DESC&user={proxy_address}&mergeable=true"
        try:
            response = requests.get(mergeable_url, timeout=10)
            if response.status_code == 200:
                mergeable_positions = response.json() or []
                all_positions.extend(mergeable_positions)
                logger.info(f"Found {len(mergeable_positions)} mergeable positions")
        except Exception as e:
            logger.warning(f"Error fetching mergeable positions: {e}")
        
        # Then, fetch all positions for redemption after resolution
        all_url = f"https://data-api.polymarket.com/positions?sizeThreshold=1&limit=100&sortBy=TOKENS&sortDirection=DESC&user={proxy_address}"
        try:
            response = requests.get(all_url, timeout=10)
            if response.status_code == 200:
                positions = response.json() or []
                # Add positions not already in the list (avoid duplicates by conditionId)
                existing_conditions = {p.get("conditionId") for p in all_positions}
                for pos in positions:
                    if pos.get("conditionId") not in existing_conditions:
                        all_positions.append(pos)
        except Exception as e:
            logger.warning(f"Error fetching all positions: {e}")
        
        if not all_positions:
            return []
            
        # Filter by market slug if provided (partial match for flexibility)
        if market_slug:
            # Use partial match - market slug contains the base pattern
            base_pattern = market_slug.rsplit('-', 1)[0] if '-' in market_slug else market_slug
            filtered = [p for p in all_positions if p.get("slug") and base_pattern in p.get("slug", "")]
            if filtered:
                all_positions = filtered
            else:
                # If no match with partial, try exact match
                all_positions = [p for p in all_positions if p.get("slug") == market_slug]
            
        return all_positions
        
    except Exception as e:
        logger.error(f"Error fetching redeemable positions: {e}")
        return []


def _get_web3_and_account() -> Tuple[Web3, Account, str]:
    """
    Initialize Web3 connection and account.
    
    Returns:
        Tuple of (Web3 instance, Account instance, safe_address)
    """
    # Use RPC_URL from env, with fallback to public Polygon RPC
    rpc_url = os.getenv("RPC_URL", "https://polygon-rpc.com")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    
    private_key = os.getenv("PRIVATE_KEY")
    account = Account.from_key(private_key)
    safe_address = Web3.to_checksum_address(os.getenv("POLYMARKET_PROXY_ADDRESS"))
    
    return w3, account, safe_address


def _execute_safe_transaction(w3: Web3, account: Account, safe_address: str, 
                              to_address: str, data: bytes) -> bool:
    """
    Execute a transaction through the Safe wallet.
    
    Args:
        w3: Web3 instance
        account: Account instance
        safe_address: Safe wallet address
        to_address: Target contract address
        data: Encoded transaction data
        
    Returns:
        bool: True if transaction successful, False otherwise
    """
    try:
        safe = w3.eth.contract(address=safe_address, abi=safe_abi)
        nonce = safe.functions.nonce().call()
        
        tx_hash = safe.functions.getTransactionHash(
            Web3.to_checksum_address(to_address),
            0,
            data,
            0,  # operation: Call
            0, 0, 0,  # safeTxGas, baseGas, gasPrice
            "0x0000000000000000000000000000000000000000",  # gasToken
            "0x0000000000000000000000000000000000000000",  # refundReceiver
            nonce,
        ).call()
        
        # Sign the hash
        hash_bytes = Web3.to_bytes(
            hexstr=tx_hash.hex() if hasattr(tx_hash, "hex") else tx_hash
        )
        signature_obj = account.unsafe_sign_hash(hash_bytes)
        
        r = signature_obj.r.to_bytes(32, byteorder="big")
        s = signature_obj.s.to_bytes(32, byteorder="big")
        v = signature_obj.v.to_bytes(1, byteorder="big")
        signature = r + s + v
        
        # Build and send transaction
        tx = safe.functions.execTransaction(
            Web3.to_checksum_address(to_address),
            0,
            data,
            0, 0, 0, 0,
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            signature,
        ).build_transaction({
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": 500000,
            "gasPrice": w3.eth.gas_price,
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        
        # Wait for receipt
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        
        return receipt["status"] == 1
        
    except Exception as e:
        logger.error(f"Safe transaction failed: {e}")
        return False


def _get_position_balances(w3: Web3, safe_address: str, condition_id: str) -> Tuple[int, int]:
    """
    Get balances of both outcome positions for a condition.
    
    Args:
        w3: Web3 instance
        safe_address: Safe wallet address
        condition_id: The condition ID (bytes32 hex string)
        
    Returns:
        Tuple of (balance_yes, balance_no) in wei
    """
    ctf_contract = w3.eth.contract(
        address=Web3.to_checksum_address(CONDITIONAL_TOKENS_FRAMEWORK_ADDRESS),
        abi=ctf_abi,
    )
    
    parent_collection_id = bytes(32)
    collection_id_0 = ctf_contract.functions.getCollectionId(
        parent_collection_id, bytes.fromhex(condition_id[2:]), 1
    ).call()
    collection_id_1 = ctf_contract.functions.getCollectionId(
        parent_collection_id, bytes.fromhex(condition_id[2:]), 2
    ).call()
    
    position_id_0 = ctf_contract.functions.getPositionId(
        Web3.to_checksum_address(USDC_ADDRESS), collection_id_0
    ).call()
    position_id_1 = ctf_contract.functions.getPositionId(
        Web3.to_checksum_address(USDC_ADDRESS), collection_id_1
    ).call()
    
    balance_0 = ctf_contract.functions.balanceOf(safe_address, position_id_0).call()
    balance_1 = ctf_contract.functions.balanceOf(safe_address, position_id_1).call()
    
    return balance_0, balance_1


def _is_condition_resolved(w3: Web3, condition_id: str) -> bool:
    """
    Check if a condition has been resolved by the oracle.
    
    Args:
        w3: Web3 instance
        condition_id: The condition ID (bytes32 hex string)
        
    Returns:
        bool: True if condition is resolved, False otherwise
    """
    try:
        ctf_contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS_FRAMEWORK_ADDRESS),
            abi=ctf_abi,
        )
        # payoutNumerators returns the payout for each outcome
        # If all zeros, the market hasn't been resolved yet
        payout_denominator = ctf_contract.functions.payoutDenominator(
            bytes.fromhex(condition_id[2:])
        ).call()
        return payout_denominator > 0
    except Exception as e:
        logger.debug(f"Error checking resolution status: {e}")
        return False


def redeem_condition(condition_id: str, neg_risk: bool = False) -> bool:
    """
    Redeem positions for a condition back to USDC.
    
    Strategy:
    1. First, merge any equal YES/NO tokens (works before resolution)
    2. Then, if market is resolved, redeem remaining positions
    
    Args:
        condition_id: The condition ID (bytes32 hex string)
        neg_risk: Whether to use NEG_RISK_ADAPTER (default: False)
        
    Returns:
        bool: True if redeem successful, False otherwise
    """
    try:
        w3, account, safe_address = _get_web3_and_account()
        
        # Check if there are any positions to redeem
        balance_yes, balance_no = _get_position_balances(w3, safe_address, condition_id)
        
        if balance_yes == 0 and balance_no == 0:
            logger.debug(f"No positions to redeem for condition {condition_id[:10]}...")
            return True  # Not an error, just nothing to redeem
            
        logger.info(f"Found positions - YES: {balance_yes / 10**USDCE_DIGITS:.2f}, NO: {balance_no / 10**USDCE_DIGITS:.2f}")
        
        # Step 1: Merge equal amounts first (doesn't require resolution)
        merge_amount = min(balance_yes, balance_no)
        if merge_amount > 0:
            logger.info(f"Merging {merge_amount / 10**USDCE_DIGITS:.2f} USDC worth of equal positions...")
            if merge_condition(condition_id, neg_risk):
                # Update balances after merge
                balance_yes, balance_no = _get_position_balances(w3, safe_address, condition_id)
                logger.info(f"After merge - YES: {balance_yes / 10**USDCE_DIGITS:.2f}, NO: {balance_no / 10**USDCE_DIGITS:.2f}")
            else:
                logger.warning("Merge failed, will try to redeem if resolved")
        
        # Step 2: Check if there's anything left to redeem
        if balance_yes == 0 and balance_no == 0:
            logger.info(f"✅ All positions merged successfully for condition {condition_id[:10]}...")
            return True
            
        # Step 3: Check if market is resolved before attempting redeemPositions
        is_resolved = _is_condition_resolved(w3, condition_id)
        if not is_resolved:
            remaining_value = max(balance_yes, balance_no) / 10**USDCE_DIGITS
            logger.info(f"⏳ Market not yet resolved. Remaining {remaining_value:.2f} USDC will be redeemable after resolution.")
            return True  # Merge succeeded, redemption just needs to wait
        
        logger.info("Market resolved, redeeming remaining positions...")
        
        # Prepare the redeem transaction using redeemPositions
        ctf_contract = w3.eth.contract(abi=ctf_abi)
        
        parent_collection_id = "0x0000000000000000000000000000000000000000000000000000000000000000"
        # Index sets: 1 for YES outcome, 2 for NO outcome
        index_sets = [1, 2]
        
        data = ctf_contract.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS),
            bytes.fromhex(parent_collection_id[2:]),
            bytes.fromhex(condition_id[2:]),
            index_sets,
        )._encode_transaction_data()
        
        to = NEG_RISK_ADAPTER_ADDRESS if neg_risk else CONDITIONAL_TOKENS_FRAMEWORK_ADDRESS
        
        success = _execute_safe_transaction(
            w3, account, safe_address, to, bytes.fromhex(data[2:])
        )
        
        if success:
            total_redeemed = (balance_yes + balance_no) / 10**USDCE_DIGITS
            logger.info(f"✅ Redeemed positions for condition {condition_id[:10]}... (up to {total_redeemed:.2f} USDC)")
            return True
        else:
            logger.error(f"Redeem transaction failed for condition {condition_id[:10]}...")
            return False
            
    except Exception as e:
        logger.error(f"Error redeeming condition {condition_id[:10]}...: {e}")
        return False


def merge_condition(condition_id: str, neg_risk: bool = False) -> bool:
    """
    Merge equal amounts of YES/NO tokens for a condition back to USDC.
    
    This only works when you have equal amounts of both outcomes.
    For unequal positions after trading, use redeem_condition instead.
    
    Args:
        condition_id: The condition ID (bytes32 hex string)
        neg_risk: Whether to use NEG_RISK_ADAPTER (default: False)
        
    Returns:
        bool: True if merge successful, False otherwise
    """
    try:
        w3, account, safe_address = _get_web3_and_account()
        
        # Get the amount to merge (minimum of both balances)
        balance_yes, balance_no = _get_position_balances(w3, safe_address, condition_id)
        amount_wei = min(balance_yes, balance_no)
        
        if amount_wei == 0:
            logger.debug(f"No tokens to merge for condition {condition_id[:10]}...")
            return True  # Not an error, just nothing to merge
            
        # Prepare the merge transaction
        ctf_contract = w3.eth.contract(abi=ctf_abi)
        
        parent_collection_id = "0x0000000000000000000000000000000000000000000000000000000000000000"
        partition = [1, 2]
        
        data = ctf_contract.functions.mergePositions(
            Web3.to_checksum_address(USDC_ADDRESS),
            bytes.fromhex(parent_collection_id[2:]),
            bytes.fromhex(condition_id[2:]),
            partition,
            amount_wei,
        )._encode_transaction_data()
        
        to = NEG_RISK_ADAPTER_ADDRESS if neg_risk else CONDITIONAL_TOKENS_FRAMEWORK_ADDRESS
        
        success = _execute_safe_transaction(
            w3, account, safe_address, to, bytes.fromhex(data[2:])
        )
        
        if success:
            amount_usdc = amount_wei / 10**USDCE_DIGITS
            logger.info(f"✅ Merged {amount_usdc:.2f} USDC for condition {condition_id[:10]}...")
            return True
        else:
            logger.error(f"Merge transaction failed for condition {condition_id[:10]}...")
            return False
            
    except Exception as e:
        logger.error(f"Error merging condition {condition_id[:10]}...: {e}")
        return False


def redeem_market_positions(market_slug: str) -> Tuple[int, int]:
    """
    Redeem all traded positions for a specific market.
    
    This function should be called when a market closes/resolves to convert
    held positions back to USDC before moving to the next market.
    
    Uses redeemPositions which handles unequal YES/NO positions after resolution.
    
    Args:
        market_slug: The market slug to redeem positions for
        
    Returns:
        Tuple of (successful_redeems, total_conditions)
    """
    logger.info(f"🔄 Checking for redeemable positions in market: {market_slug}")
    
    positions = get_redeemable_positions(market_slug)
    
    if not positions:
        logger.info("No positions to redeem for this market")
        return (0, 0)
    
    # Get unique condition IDs
    condition_ids = set(p.get("conditionId") for p in positions if p.get("conditionId"))
    
    if not condition_ids:
        logger.info("No condition IDs found in positions")
        return (0, 0)
    
    logger.info(f"Found {len(condition_ids)} condition(s) to redeem")
    
    successful = 0
    for condition_id in condition_ids:
        if redeem_condition(condition_id):
            successful += 1
            
    logger.info(f"✅ Redemption complete: {successful}/{len(condition_ids)} conditions redeemed successfully")
    
    return (successful, len(condition_ids))


def redeem_all_positions() -> Tuple[int, int]:
    """
    Redeem all available positions across all markets.
    
    Returns:
        Tuple of (successful_redeems, total_conditions)
    """
    logger.info("🔄 Checking for all redeemable positions")
    
    positions = get_redeemable_positions()
    
    if not positions:
        logger.info("No positions to redeem")
        return (0, 0)
    
    # Get unique condition IDs
    condition_ids = set(p.get("conditionId") for p in positions if p.get("conditionId"))
    
    if not condition_ids:
        logger.info("No condition IDs found in positions")
        return (0, 0)
    
    logger.info(f"Found {len(condition_ids)} condition(s) to redeem")
    
    successful = 0
    for condition_id in condition_ids:
        if redeem_condition(condition_id):
            successful += 1
            
    logger.info(f"✅ Redemption complete: {successful}/{len(condition_ids)} conditions redeemed successfully")
    
    return (successful, len(condition_ids))


def merge_balanced_positions() -> Tuple[int, float]:
    """
    Merge all balanced positions (equal YES/NO tokens) to recover USDC immediately.
    
    This function should be called after a market closes to free up capital
    without waiting for market resolution. Only merges balanced portions;
    unbalanced positions (the "speculation" part) will wait for resolution.
    
    Returns:
        Tuple of (number_of_merges, total_usdc_recovered)
    """
    logger.info("💰 Checking for balanced positions to merge immediately...")
    
    try:
        proxy_address = os.getenv('POLYMARKET_PROXY_ADDRESS')
        
        # Fetch only mergeable positions (balanced Yes/No)
        url = f"https://data-api.polymarket.com/positions?sizeThreshold=1&limit=100&sortBy=TOKENS&sortDirection=DESC&user={proxy_address}&mergeable=true"
        
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch mergeable positions: HTTP {response.status_code}")
            return (0, 0.0)
            
        positions = response.json()
        if not positions:
            logger.info("No balanced positions to merge")
            return (0, 0.0)
            
        # Get unique condition IDs from mergeable positions
        condition_ids = set(p.get("conditionId") for p in positions if p.get("conditionId"))
        
        if not condition_ids:
            logger.info("No condition IDs found in mergeable positions")
            return (0, 0.0)
            
        logger.info(f"🔄 Found {len(condition_ids)} balanced position(s) to merge")
        
        successful = 0
        total_recovered = 0.0
        
        w3, account, safe_address = _get_web3_and_account()
        
        for condition_id in condition_ids:
            try:
                # Get balances before merge
                balance_yes, balance_no = _get_position_balances(w3, safe_address, condition_id)
                merge_amount = min(balance_yes, balance_no)
                
                if merge_amount > 0:
                    usdc_amount = merge_amount / 10**USDCE_DIGITS
                    logger.info(f"Merging {usdc_amount:.2f} USDC from condition {condition_id[:10]}...")
                    
                    if merge_condition(condition_id):
                        successful += 1
                        total_recovered += usdc_amount
                        logger.info(f"✅ Successfully merged {usdc_amount:.2f} USDC")
                    else:
                        logger.warning(f"Failed to merge condition {condition_id[:10]}...")
                        
            except Exception as e:
                logger.error(f"Error merging condition {condition_id[:10]}...: {e}")
                
        if successful > 0:
            logger.info(f"💰 Merge complete: Recovered {total_recovered:.2f} USDC from {successful} positions")
        else:
            logger.info("No positions were merged")
            
        return (successful, total_recovered)
        
    except Exception as e:
        logger.error(f"Error in merge_balanced_positions: {e}")
        return (0, 0.0)

