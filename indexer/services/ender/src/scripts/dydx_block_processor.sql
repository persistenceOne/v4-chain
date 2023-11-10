CREATE OR REPLACE FUNCTION dydx_block_processor(block jsonb) RETURNS jsonb AS $$
DECLARE
    USDC_ASSET_ID constant text = '0';

    block_height int = (block->'height')::int;
    block_time timestamp = (block->>'time')::timestamp;
    event_ jsonb;
    rval jsonb[];
    event_index int;
    transaction_index int;
    event_data jsonb;
BEGIN
    rval = ARRAY[jsonb_array_length(block->'events')];

    /** Batch handlers */
    FOR i in 0..jsonb_array_length(block->'events')-1 LOOP
        event_ = jsonb_array_element(block->'events', i);
        transaction_index = dydx_tendermint_event_to_transaction_index(event_);
        event_index = (event_->'eventIndex')::int;
        event_data = event_->'dataBytes';
        CASE event_->'subtype'
            WHEN '"order_fill"'::jsonb THEN
                IF event_data->'order' IS NOT NULL THEN
                    rval[i] = jsonb_build_object(
                            'makerOrder',
                            dydx_order_fill_handler_per_order('makerOrder', block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index), 'MAKER', 'LIMIT', USDC_ASSET_ID, 'NOT_CANCELED'),
                            'order',
                            dydx_order_fill_handler_per_order('order', block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index), 'TAKER', 'LIMIT', USDC_ASSET_ID, 'NOT_CANCELED'));
                ELSE
                    rval[i] = jsonb_build_object(
                            'makerOrder',
                            dydx_liquidation_fill_handler_per_order('makerOrder', block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index), 'MAKER', 'LIQUIDATION', USDC_ASSET_ID),
                            'liquidationOrder',
                            dydx_liquidation_fill_handler_per_order('liquidationOrder', block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index), 'TAKER', 'LIQUIDATED', USDC_ASSET_ID));
                END IF;
            WHEN '"subaccount_update"'::jsonb THEN
                rval[i] = dydx_subaccount_update_handler(block_height, block_time, event_data, event_index, transaction_index);
            WHEN '"transfer"'::jsonb THEN
                rval[i] = dydx_transfer_handler(block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index));
            WHEN '"stateful_order"'::jsonb THEN
                rval[i] = dydx_stateful_order_handler(block_height, block_time, event_data);
            WHEN '"funding_values"'::jsonb THEN
                rval[i] = dydx_funding_handler(block_height, block_time, event_data, event_index, transaction_index);
            WHEN '"deleveraging"'::jsonb THEN
                rval[i] = dydx_deleveraging_handler(block_height, block_time, event_data, event_index, transaction_index, jsonb_array_element_text(block->'txHashes', transaction_index));
            ELSE
                NULL;
        END CASE;
    END LOOP;

    /** Sync handlers */
    FOR i in 0..jsonb_array_length(block->'events')-1 LOOP
        event_ = jsonb_array_element(block->'events', i);
        transaction_index = dydx_tendermint_event_to_transaction_index(event_);
        event_index = (event_->'eventIndex')::int;
        event_data = event_->'dataBytes';
        CASE event_->'subtype'
            WHEN '"market"'::jsonb THEN
                IF event_data->'priceUpdate' IS NOT NULL THEN
                    rval[i] = dydx_market_price_update_handler(block_height, block_time, event_data);
                ELSIF event_data->'marketCreate' IS NOT NULL THEN
                    rval[i] = dydx_market_create_handler(event_data);
                ELSIF event_data->'marketModify' IS NOT NULL THEN
                    rval[i] = dydx_market_modify_handler(event_data);
                ELSE
                    RAISE EXCEPTION 'Unknown market event %', event_;
                END IF;
            WHEN '"asset"'::jsonb THEN
                rval[i] = dydx_asset_create_handler(event_data);
            WHEN '"perpetual_market"'::jsonb THEN
                rval[i] = dydx_perpetual_market_handler(event_data);
            WHEN '"liquidity_tier"'::jsonb THEN
                rval[i] = dydx_liquidity_tier_handler(event_data);
            WHEN '"update_perpetual"'::jsonb THEN
                rval[i] = dydx_update_perpetual_handler(event_data);
            WHEN '"update_clob_pair"'::jsonb THEN
                rval[i] = dydx_update_clob_pair_handler(event_data);
            ELSE
                NULL;
        END CASE;
    END LOOP;

    RETURN to_jsonb(rval);
END;
$$ LANGUAGE plpgsql;
