// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live execution client implementation for the Binance Spot adapter.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    cache::fifo::FifoCache,
    clients::ExecutionClient,
    execution_write::{
        ExecutionEndpointPath, ExecutionHttpMethod, ExecutionOrderResponseMode, ExecutionProduct,
        ExecutionRoute, ExecutionTimeInForce, ExecutionTransportTargetViewV1,
        ExecutionWriteContext, ExecutionWriteElementViewV1, ExecutionWriteGateError,
        ExecutionWriteGateHandle, ExecutionWriteKind, ExecutionWritePayloadViewV1,
        FinalWritePayloadDigest, TransportOutcomeClass,
    },
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder,
        CorrelatedTruthReporter, GenerateBinanceTruthReport, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GenerateOrderStatusReportsBuilder,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder, ModifyOrder,
        QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList, TruthReportError,
    },
};
use nautilus_core::{
    MUTEX_POISONED, Params, UUID4, UnixNanos,
    datetime::mins_to_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};

const RETRY_OF_PARAM: &str = "sae_retry_of";

fn retry_digest(params: Option<&Params>) -> anyhow::Result<Option<FinalWritePayloadDigest>> {
    let Some(value) = params.and_then(|params| params.get_str(RETRY_OF_PARAM)) else {
        return Ok(None);
    };
    anyhow::ensure!(
        value.len() == 64 && value.is_ascii(),
        "sae_retry_of must be 64 lowercase hex characters"
    );
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        let pair = &value[offset..offset + 2];
        anyhow::ensure!(
            pair.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "sae_retry_of must be 64 lowercase hex characters"
        );
        *byte = u8::from_str_radix(pair, 16)?;
    }
    Ok(Some(FinalWritePayloadDigest(digest)))
}
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{ContingencyType, LiquiditySide, OmsType, OrderStatus, OrderType},
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny,
        OrderExpired, OrderFilled, OrderModifyRejected, OrderRejected, OrderUpdated,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, TraderId, Venue, VenueOrderId,
    },
    instruments::Instrument,
    orders::{Order, OrderAny},
    reports::{
        BinanceModeProof, BinanceTruthReport, ExactOrderQueryResult, ExecutionMassStatus,
        FillReport, OrderStatusReport, PositionStatusReport, PrivateStreamHealth,
    },
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};

#[derive(Clone, Debug)]
struct BinanceSpotTruthReporter {
    client_id: ClientId,
    account_id: AccountId,
    clock: &'static AtomicTime,
    http_client: BinanceSpotHttpClient,
    dispatch_state: Arc<WsDispatchState>,
}

#[async_trait]
impl CorrelatedTruthReporter for BinanceSpotTruthReporter {
    async fn generate_truth_report(
        &self,
        request: GenerateBinanceTruthReport,
    ) -> Result<BinanceTruthReport, TruthReportError> {
        if request.client_id != self.client_id {
            return Err(TruthReportError::ClientIdMismatch {
                expected: self.client_id,
                actual: request.client_id,
            });
        }
        if request.account_id != self.account_id {
            return Err(TruthReportError::AccountIdMismatch {
                expected: self.account_id,
                actual: request.account_id,
            });
        }

        let account_info = self
            .http_client
            .request_account_info()
            .await
            .map_err(|error| TruthReportError::Account(error.to_string()))?;
        let balances = account_info
            .to_account_state(self.account_id, request.ts_init)
            .balances;
        let open_orders = self
            .http_client
            .request_order_status_reports(self.account_id, None, None, None, true, None)
            .await
            .map_err(|error| TruthReportError::OpenOrders(error.to_string()))?;

        let mut exact_orders = BTreeMap::new();
        for client_order_id in request.exact_order_ids {
            let instrument_id = self
                .dispatch_state
                .order_identities
                .get(&client_order_id)
                .map(|identity| identity.instrument_id)
                .ok_or(TruthReportError::MissingOrderOrigin(client_order_id))?;
            let exact = self
                .http_client
                .request_order_status_report(
                    self.account_id,
                    instrument_id,
                    None,
                    Some(client_order_id),
                )
                .await
                .map_err(|error| TruthReportError::ExactOrder {
                    client_order_id,
                    detail: error.to_string(),
                })?
                .map_or(ExactOrderQueryResult::NotFound, |report| {
                    ExactOrderQueryResult::Found(Box::new(report))
                });
            exact_orders.insert(client_order_id, exact);
        }

        Ok(BinanceTruthReport {
            request_id: request.request_id,
            client_id: self.client_id,
            account_id: self.account_id,
            mode: BinanceModeProof::Spot {
                can_trade: account_info.can_trade,
                account_type: account_info.account_type,
            },
            balances,
            positions: Vec::new(),
            open_orders,
            exact_orders,
            ts_received: self.clock.get_time_ns(),
        })
    }
}
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use ustr::Ustr;

use super::websocket::trading::{
    client::BinanceSpotWsTradingClient,
    messages::BinanceSpotWsTradingMessage,
    parse::{
        parse_spot_account_position, parse_spot_exec_report_to_fill,
        parse_spot_exec_report_to_order_status,
    },
    user_data::{BinanceSpotExecutionReport, BinanceSpotExecutionType},
};
use crate::{
    common::{
        consts::{
            BINANCE_GTX_ORDER_REJECT_CODE, BINANCE_NAUTILUS_SPOT_BROKER_ID,
            BINANCE_NEW_ORDER_REJECTED_CODE, BINANCE_SPOT_POST_ONLY_REJECT_MSG,
            BINANCE_STATUS_UNKNOWN_CODE, BINANCE_UNEXPECTED_RESPONSE_CODE, BINANCE_VENUE,
        },
        credential::resolve_credentials,
        dispatch::{
            OrderIdentity, PendingOperation, PendingRequest, WsDispatchState,
            ensure_accepted_emitted,
        },
        encoder::{decode_broker_id, encode_broker_id},
        enums::{BinanceSide, BinanceTimeInForce},
        parse::{
            parse_required_decimal, parse_required_price_at_precision,
            parse_required_quantity_at_precision,
        },
        private_stream::PrivateStreamHealthHandle,
    },
    config::BinanceExecClientConfig,
    spot::{
        enums::{
            BinanceCancelReplaceMode, BinanceOrderResponseType, BinanceSpotOrderType,
            order_type_to_binance_spot, time_in_force_to_binance_spot,
        },
        http::{
            client::BinanceSpotHttpClient,
            error::BinanceSpotHttpError,
            query::{
                CancelOrderParams, CancelReplaceOrderParams, NewOcoOrderListParams, NewOrderParams,
            },
        },
    },
};

/// Live execution client for Binance Spot trading.
///
/// Implements the [`ExecutionClient`] trait for order management on Binance Spot
/// and Spot Margin markets. First-release order mutations use signed HTTP while the
/// WebSocket User Data Stream continues to provide real-time execution events.
#[derive(Debug)]
pub struct BinanceSpotExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: BinanceExecClientConfig,
    emitter: ExecutionEventEmitter,
    dispatch_state: Arc<WsDispatchState>,
    http_client: BinanceSpotHttpClient,
    ws_trading_client: Option<BinanceSpotWsTradingClient>,
    ws_trading_handle: Mutex<Option<JoinHandle<()>>>,
    ws_authenticated: Arc<tokio::sync::Notify>,
    ws_user_data_subscribed: Arc<tokio::sync::Notify>,
    private_stream_health: PrivateStreamHealthHandle,
    truth_reporter: Arc<BinanceSpotTruthReporter>,
    write_gate: Option<ExecutionWriteGateHandle>,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl BinanceSpotExecutionClient {
    /// Creates a new [`BinanceSpotExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client fails to initialize or credentials are missing.
    pub fn new(core: ExecutionClientCore, config: BinanceExecClientConfig) -> anyhow::Result<Self> {
        Self::new_inner(core, config, None)
    }

    /// Creates a production client with the required final execution-write gate.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client fails to initialize or credentials are missing.
    pub fn new_with_write_gate(
        core: ExecutionClientCore,
        config: BinanceExecClientConfig,
        write_gate: ExecutionWriteGateHandle,
    ) -> anyhow::Result<Self> {
        Self::new_inner(core, config, Some(write_gate))
    }

    fn new_inner(
        core: ExecutionClientCore,
        config: BinanceExecClientConfig,
        write_gate: Option<ExecutionWriteGateHandle>,
    ) -> anyhow::Result<Self> {
        let (api_key, api_secret) = resolve_credentials(
            config.api_key.clone(),
            config.api_secret.clone(),
            config.environment,
            config.product_type,
        )?;

        let clock = get_atomic_clock_realtime();

        let http_client = BinanceSpotHttpClient::new(
            config.environment,
            clock,
            Some(api_key.clone()),
            Some(api_secret.clone()),
            config.base_url_http.clone(),
            Some(5_000), // reviewed first-release recv_window
            None,        // timeout_secs
            None,        // proxy_url
        )
        .context("failed to construct Binance Spot HTTP client")?;
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        let ws_trading_client = if config.use_ws_trading {
            Some(BinanceSpotWsTradingClient::new(
                config.base_url_ws_trading.clone(),
                api_key,
                api_secret,
                None, // heartbeat
                config.transport_backend,
            ))
        } else {
            None
        };

        let client_id = core.client_id;

        let dispatch_state = Arc::new(WsDispatchState::default());
        let truth_reporter = Arc::new(BinanceSpotTruthReporter {
            client_id,
            account_id: core.account_id,
            clock,
            http_client: http_client.clone(),
            dispatch_state: dispatch_state.clone(),
        });

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            dispatch_state,
            http_client,
            ws_trading_client,
            ws_trading_handle: Mutex::new(None),
            ws_authenticated: Arc::new(tokio::sync::Notify::new()),
            ws_user_data_subscribed: Arc::new(tokio::sync::Notify::new()),
            private_stream_health: PrivateStreamHealthHandle::new(client_id),
            truth_reporter,
            write_gate,
            pending_tasks: Mutex::new(Vec::new()),
        })
    }

    async fn refresh_account_state(&self) -> anyhow::Result<AccountState> {
        self.http_client
            .request_account_state(self.core.account_id)
            .await
    }

    fn update_account_state(&self) {
        let http_client = self.http_client.clone();
        let account_id = self.core.account_id;
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let account_state = http_client.request_account_state(account_id).await?;
            let ts_now = clock.get_time_ns();
            emitter.emit_account_state(
                account_state.balances.clone(),
                account_state.margins.clone(),
                account_state.is_reported,
                ts_now,
            );
            Ok(())
        });
    }

    /// Returns whether the WS trading client is connected and active.
    fn ws_trading_active(&self) -> bool {
        let dispatch_running = self
            .ws_trading_handle
            .lock()
            .expect(MUTEX_POISONED)
            .as_ref()
            .is_some_and(|handle| !handle.is_finished());

        self.ws_trading_client
            .as_ref()
            .is_some_and(|client| client.is_active())
            && dispatch_running
    }

    fn submit_order_internal(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();
        let order_side = order.order_side();
        let order_type = order.order_type();
        let quantity = order.quantity();
        let time_in_force = order.time_in_force();
        let price = order.price();
        let trigger_price = order.trigger_price();
        let is_post_only = order.is_post_only();
        let is_quote_quantity = order.is_quote_quantity();
        let display_qty = order.display_qty();
        let clock = self.clock;
        let ts_init = self.clock.get_time_ns();

        anyhow::ensure!(
            matches!(order_type, OrderType::Market | OrderType::Limit),
            "first-release Binance Spot rejects conditional order type {order_type:?}"
        );
        anyhow::ensure!(
            !is_quote_quantity,
            "first-release Binance Spot rejects quote-order quantity"
        );
        anyhow::ensure!(
            trigger_price.is_none(),
            "first-release Binance Spot rejects trigger price"
        );
        anyhow::ensure!(
            display_qty.is_none(),
            "first-release Binance Spot rejects display quantity"
        );
        if order_type == OrderType::Limit {
            anyhow::ensure!(price.is_some(), "Binance Spot limit order requires a price");
        } else {
            anyhow::ensure!(price.is_none(), "Binance Spot market order forbids a price");
        }

        let retry_of = retry_digest(cmd.params.as_ref())?;
        let write_kind = if retry_of.is_some() {
            ExecutionWriteKind::Retry
        } else {
            ExecutionWriteKind::Submit
        };
        let final_time_in_force = final_time_in_force(order_type, time_in_force, is_post_only)?;
        let final_payload = ExecutionWritePayloadViewV1 {
            schema_version: 1,
            kind: write_kind,
            client_id: self.core.client_id,
            account_id,
            product: ExecutionProduct::Spot,
            route: ExecutionRoute::BinanceSpot,
            transport: ExecutionTransportTargetViewV1::Http {
                method: ExecutionHttpMethod::Post,
                path: ExecutionEndpointPath::SpotOrder,
                recv_window_ms: 5_000,
            },
            elements: vec![ExecutionWriteElementViewV1::Submit {
                batch_index: 0,
                instrument_id,
                client_order_id,
                side: order_side,
                order_type,
                time_in_force: final_time_in_force,
                quantity,
                price,
                trigger_price,
                quote_quantity: None,
                display_quantity: display_qty,
                reduce_only: false,
                post_only: is_post_only,
                position_side: None,
                close_position: None,
                working_type: None,
                price_protect: None,
                response_mode: ExecutionOrderResponseMode::SpotFull,
                good_till_date_ms: None,
                self_trade_prevention_mode: None,
                trailing_delta: None,
                activation_price: None,
                callback_rate: None,
                price_match: None,
                strategy_id: None,
                strategy_type: None,
            }],
            retry_of,
        };
        let write_context = ExecutionWriteContext {
            command_id: cmd.command_id,
            client_id: self.core.client_id,
            account_id,
            route: ExecutionRoute::BinanceSpot,
            instrument_id: Some(instrument_id),
            client_order_ids: vec![client_order_id],
            kind: write_kind,
        };

        // Register identity for tracked/external dispatch routing
        self.dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id,
                order_side,
                order_type,
                price,
                quantity,
            },
        );

        if self.config.use_ws_order_transport && self.ws_trading_active() {
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let dispatch_state = self.dispatch_state.clone();
            let params =
                build_new_order_params(&order, client_order_id, is_post_only, is_quote_quantity)?;

            // Pre-register before sending to avoid response racing the insert
            let request_id = ws_client.next_request_id();
            dispatch_state.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    client_order_id,
                    venue_order_id: None,
                    operation: PendingOperation::Place,
                },
            );

            self.spawn_task("submit_order_ws", async move {
                if let Err(e) = ws_client
                    .place_order_with_id(request_id.clone(), params)
                    .await
                {
                    dispatch_state.pending_requests.remove(&request_id);
                    log::warn!(
                        "WS submit request failed for {client_order_id}, awaiting reconciliation: {e}"
                    );
                    anyhow::bail!("WS submit order failed: {e}");
                }
                Ok(())
            });
        } else {
            let http_client = self.http_client.clone();
            let dispatch_state = self.dispatch_state.clone();
            let write_gate = self.write_gate.clone();
            log::debug!("Using writer-authorized HTTP transport for submit_order");

            self.spawn_task("submit_order_http", async move {
                let write_gate = write_gate.ok_or(ExecutionWriteGateError::MissingGate)?;
                final_payload.validate_first_release(&write_context)?;
                let mut permit = write_gate
                    .acquire(&write_context, &final_payload)
                    .await?;
                permit.arm_before_transport(&final_payload)?;
                let result = http_client
                    .submit_order(
                        account_id,
                        instrument_id,
                        client_order_id,
                        order_side,
                        order_type,
                        quantity,
                        time_in_force,
                        price,
                        trigger_price,
                        is_post_only,
                        is_quote_quantity,
                        display_qty,
                    )
                    .await;

                let outcome = match &result {
                    Ok(_) => TransportOutcomeClass::AcceptedNonterminal,
                    Err(error)
                        if is_structured_venue_rejection(error)
                            || is_local_command_failure(error) =>
                    {
                        TransportOutcomeClass::RejectedDefinitive
                    }
                    Err(_) => TransportOutcomeClass::OutcomeUnknown,
                };
                permit.record_outcome(outcome);

                match result {
                    Ok(report) => {
                        dispatch_state.insert_accepted(client_order_id);
                        let accepted = OrderAccepted::new(
                            trader_id,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            report.venue_order_id,
                            account_id,
                            UUID4::new(),
                            ts_init,
                            ts_init,
                            false,
                        );
                        event_emitter.send_order_event(OrderEventAny::Accepted(accepted));
                    }
                    Err(e) => {
                        if is_ambiguous_submit_error(&e) {
                            log::warn!(
                                "Ambiguous submit failure for {client_order_id}, awaiting reconciliation: {e}"
                            );
                        } else if is_structured_venue_rejection(&e)
                            || is_local_command_failure(&e)
                        {
                            let due_post_only = e
                                .downcast_ref::<BinanceSpotHttpError>()
                                .is_some_and(is_spot_post_only_rejection);
                            dispatch_state.cleanup_terminal(client_order_id);
                            let rejected = OrderRejected::new(
                                trader_id,
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                account_id,
                                format!("submit-order-error: {e}").into(),
                                UUID4::new(),
                                ts_init,
                                clock.get_time_ns(),
                                false,
                                due_post_only,
                            );
                            event_emitter.send_order_event(OrderEventAny::Rejected(rejected));
                        } else {
                            log::warn!(
                                "Ambiguous submit failure for {client_order_id}, awaiting reconciliation: {e}"
                            );
                        }
                        return Err(e);
                    }
                }
                Ok(())
            });
        }

        Ok(())
    }

    fn cancel_order_internal(&self, cmd: &CancelOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.venue_order_id.is_none(),
            "first-release Binance Spot cancel requires exact ClientOrderId only"
        );
        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;
        let command = cmd.clone();
        let final_payload = ExecutionWritePayloadViewV1 {
            schema_version: 1,
            kind: ExecutionWriteKind::Cancel,
            client_id: self.core.client_id,
            account_id,
            product: ExecutionProduct::Spot,
            route: ExecutionRoute::BinanceSpot,
            transport: ExecutionTransportTargetViewV1::Http {
                method: ExecutionHttpMethod::Delete,
                path: ExecutionEndpointPath::SpotOrder,
                recv_window_ms: 5_000,
            },
            elements: vec![ExecutionWriteElementViewV1::Cancel {
                batch_index: 0,
                instrument_id: command.instrument_id,
                client_order_id: command.client_order_id,
                venue_order_id: None,
                cancel_request_client_order_id: None,
            }],
            retry_of: None,
        };
        let write_context = ExecutionWriteContext {
            command_id: command.command_id,
            client_id: self.core.client_id,
            account_id,
            route: ExecutionRoute::BinanceSpot,
            instrument_id: Some(command.instrument_id),
            client_order_ids: vec![command.client_order_id],
            kind: ExecutionWriteKind::Cancel,
        };

        if self.config.use_ws_order_transport && self.ws_trading_active() {
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let dispatch_state = self.dispatch_state.clone();
            let params = build_cancel_order_params(&command);

            // Pre-register before sending to avoid response racing the insert
            let request_id = ws_client.next_request_id();
            dispatch_state.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    client_order_id: command.client_order_id,
                    venue_order_id: command.venue_order_id,
                    operation: PendingOperation::Cancel,
                },
            );

            self.spawn_task("cancel_order_ws", async move {
                if let Err(e) = ws_client
                    .cancel_order_with_id(request_id.clone(), params)
                    .await
                {
                    dispatch_state.pending_requests.remove(&request_id);
                    log::warn!(
                        "WS cancel request failed for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                    anyhow::bail!("WS cancel order failed: {e}");
                }
                Ok(())
            });
        } else {
            let http_client = self.http_client.clone();
            let dispatch_state = self.dispatch_state.clone();
            let write_gate = self.write_gate.clone();
            log::debug!("Using writer-authorized HTTP transport for cancel_order");

            self.spawn_task("cancel_order_http", async move {
                let write_gate = write_gate.ok_or(ExecutionWriteGateError::MissingGate)?;
                final_payload.validate_first_release(&write_context)?;
                let mut permit = write_gate.acquire(&write_context, &final_payload).await?;
                permit.arm_before_transport(&final_payload)?;
                let result = http_client
                    .cancel_order(
                        command.instrument_id,
                        command.venue_order_id,
                        Some(command.client_order_id),
                    )
                    .await;

                let outcome = match &result {
                    Ok(_) => TransportOutcomeClass::AcceptedNonterminal,
                    Err(error) if is_structured_venue_rejection(error) => {
                        TransportOutcomeClass::RejectedDefinitive
                    }
                    Err(_) => TransportOutcomeClass::OutcomeUnknown,
                };
                permit.record_outcome(outcome);

                match result {
                    Ok(venue_order_id) => {
                        dispatch_state.cleanup_terminal(command.client_order_id);
                        let ts_now = clock.get_time_ns();
                        let canceled_event = OrderCanceled::new(
                            trader_id,
                            command.strategy_id,
                            command.instrument_id,
                            command.client_order_id,
                            UUID4::new(),
                            ts_now,
                            ts_now,
                            false,
                            Some(venue_order_id),
                            Some(account_id),
                        );
                        event_emitter.send_order_event(OrderEventAny::Canceled(canceled_event));
                    }
                    Err(e) => {
                        if is_structured_venue_rejection(&e) {
                            let ts_now = clock.get_time_ns();
                            let rejected_event = OrderCancelRejected::new(
                                trader_id,
                                command.strategy_id,
                                command.instrument_id,
                                command.client_order_id,
                                format!("cancel-order-error: {e}").into(),
                                UUID4::new(),
                                ts_now,
                                ts_now,
                                false,
                                command.venue_order_id,
                                Some(account_id),
                            );
                            event_emitter
                                .send_order_event(OrderEventAny::CancelRejected(rejected_event));
                        } else if is_local_command_failure(&e) {
                            log::warn!(
                                "Cancel command failed local validation for {}: {e}",
                                command.client_order_id
                            );
                        } else {
                            log::warn!(
                                "Ambiguous cancel failure for {}, awaiting reconciliation: {e}",
                                command.client_order_id
                            );
                        }
                        return Err(e);
                    }
                }
                Ok(())
            });
        }
        Ok(())
    }

    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        crate::common::execution::spawn_task(&self.pending_tasks, description, fut);
    }

    fn abort_pending_tasks(&self) {
        crate::common::execution::abort_pending_tasks(&self.pending_tasks);
    }

    async fn enter_http_only_execution_mode(
        &mut self,
        mut ws_trading: BinanceSpotWsTradingClient,
        reason: &str,
        generation: u64,
    ) -> anyhow::Result<()> {
        self.private_stream_health.fail(generation, reason);
        log::error!(
            "{reason}; entering Spot HTTP-only execution mode. Order commands use HTTP responses; execution reconciliation requires explicit queries until WS trading is re-enabled"
        );

        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }
        ws_trading.disconnect().await;
        self.ws_trading_client = Some(ws_trading);

        if self.config.require_ws_trading {
            anyhow::bail!("required Spot private WebSocket failed: {reason}");
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BinanceSpotExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *BINANCE_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn correlated_truth_reporter(&self) -> Option<Arc<dyn CorrelatedTruthReporter>> {
        Some(self.truth_reporter.clone())
    }

    fn private_stream_health(&self) -> Option<PrivateStreamHealth> {
        Some(self.private_stream_health.snapshot())
    }

    fn mark_private_stream_reconciled(&self, generation: u64, ts_now: UnixNanos) -> bool {
        self.private_stream_health.ready(generation, ts_now)
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        let stream_generation = self.private_stream_health.begin_reconnect();

        if self.config.require_ws_trading && self.ws_trading_client.is_none() {
            self.private_stream_health.fail(
                stream_generation,
                "require_ws_trading=true but use_ws_trading=false",
            );
            anyhow::bail!("require_ws_trading=true requires use_ws_trading=true");
        }

        // Load instruments if not already done
        if !self.core.instruments_initialized() {
            let instruments = self
                .http_client
                .request_instruments()
                .await
                .context("failed to request Binance Spot instruments")?;

            if instruments.is_empty() {
                log::warn!("No instruments returned for Binance Spot");
            } else {
                log::debug!("Loaded {} Spot instruments", instruments.len());
                self.http_client.cache_instruments(instruments);
            }

            self.core.set_instruments_initialized();
        }

        // Request initial account state
        let account_state = self
            .refresh_account_state()
            .await
            .context("failed to request Binance account state")?;

        if !account_state.balances.is_empty() {
            log::debug!(
                "Received account state with {} balance(s)",
                account_state.balances.len()
            );
        }

        self.emitter.send_account_state(account_state);

        // Wait for account to be registered in cache before completing connect
        crate::common::execution::await_account_registered(&self.core, self.core.account_id, 30.0)
            .await?;

        // Connect WS trading client (primary order transport)
        if let Some(mut ws_trading) = self.ws_trading_client.take() {
            match ws_trading.connect().await {
                Ok(()) => {
                    log::debug!("Connected to Binance Spot WS trading API");

                    let ws_trading_clone = ws_trading.clone();
                    let emitter = self.emitter.clone();
                    let account_id = self.core.account_id;
                    let clock = self.clock;
                    let http_client = self.http_client.clone();
                    let dispatch_state = self.dispatch_state.clone();
                    let treat_expired_as_canceled = self.config.treat_expired_as_canceled;
                    let ws_authenticated = self.ws_authenticated.clone();
                    let ws_user_data_subscribed = self.ws_user_data_subscribed.clone();
                    let private_stream_health = self.private_stream_health.clone();
                    let (ws_setup_error_tx, mut ws_setup_error_rx) =
                        tokio::sync::mpsc::unbounded_channel();
                    let seen_trade_ids = std::sync::Arc::new(Mutex::new(FifoCache::new()));

                    let handle = get_runtime().spawn(async move {
                        loop {
                            match ws_trading_clone.recv().await {
                                Some(msg) => {
                                    private_stream_health
                                        .heartbeat(stream_generation, clock.get_time_ns());
                                    if matches!(&msg, BinanceSpotWsTradingMessage::Reconnected) {
                                        private_stream_health.stale(
                                            stream_generation,
                                            "Spot private WebSocket reconnected; reconciliation required",
                                        );
                                    }
                                    dispatch_ws_trading_message(
                                        msg,
                                        &emitter,
                                        &http_client,
                                        account_id,
                                        treat_expired_as_canceled,
                                        clock,
                                        &dispatch_state,
                                        &ws_authenticated,
                                        &ws_user_data_subscribed,
                                        &ws_setup_error_tx,
                                        &seen_trade_ids,
                                    );
                                }
                                None => {
                                    private_stream_health.fail(
                                        stream_generation,
                                        "Spot private dispatch loop ended",
                                    );
                                    log::warn!("WS trading dispatch loop ended");
                                    break;
                                }
                            }
                        }
                    });

                    *self.ws_trading_handle.lock().expect(MUTEX_POISONED) = Some(handle);

                    if let Err(e) = ws_trading.session_logon().await {
                        let reason = format!("WS session logon failed: {e}");
                        self.enter_http_only_execution_mode(ws_trading, &reason, stream_generation)
                            .await?;
                    } else {
                        let auth_result = wait_for_ws_setup_response(
                            Duration::from_secs(10),
                            self.ws_authenticated.notified(),
                            &mut ws_setup_error_rx,
                            "WS session authentication timed out",
                        )
                        .await;

                        if let Err(e) = auth_result {
                            self.enter_http_only_execution_mode(
                                ws_trading,
                                &e.to_string(),
                                stream_generation,
                            )
                            .await?;
                        } else if let Err(e) = ws_trading.subscribe_user_data().await {
                            self.private_stream_health.authenticated(stream_generation);
                            let reason = format!("WS user data subscribe failed: {e}");
                            self.enter_http_only_execution_mode(
                                ws_trading,
                                &reason,
                                stream_generation,
                            )
                            .await?;
                        } else {
                            self.private_stream_health.authenticated(stream_generation);
                            let subscribe_result = wait_for_ws_setup_response(
                                Duration::from_secs(10),
                                self.ws_user_data_subscribed.notified(),
                                &mut ws_setup_error_rx,
                                "WS user data subscription timed out",
                            )
                            .await;

                            if let Err(e) = subscribe_result {
                                self.enter_http_only_execution_mode(
                                    ws_trading,
                                    &e.to_string(),
                                    stream_generation,
                                )
                                .await?;
                            } else {
                                self.private_stream_health.subscribed(stream_generation);
                                self.ws_trading_client = Some(ws_trading);
                            }
                        }
                    }
                }
                Err(e) => {
                    let reason = format!("Failed to connect WS trading API: {e}");
                    self.enter_http_only_execution_mode(ws_trading, &reason, stream_generation)
                        .await?;
                }
            }
        }

        let stream_health = self.private_stream_health.snapshot();
        if self.config.require_ws_trading
            && (!stream_health.authenticated || !stream_health.subscribed)
        {
            anyhow::bail!("required Spot private WebSocket is not ready");
        }

        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        // Abort WS trading task and disconnect
        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }

        if let Some(ref mut ws_trading) = self.ws_trading_client {
            ws_trading.disconnect().await;
        }

        self.abort_pending_tasks();

        self.private_stream_health.stale(
            self.private_stream_health.current_generation(),
            "Spot execution client disconnected",
        );

        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        self.update_account_state();
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        log::debug!("query_order: client_order_id={}", cmd.client_order_id);

        let http_client = self.http_client.clone();
        let command = cmd;
        let event_emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let treat_expired_as_canceled = self.config.treat_expired_as_canceled;

        self.spawn_task("query_order", async move {
            let result = http_client
                .request_order_status_report(
                    account_id,
                    command.instrument_id,
                    command.venue_order_id,
                    Some(command.client_order_id),
                )
                .await;

            match result {
                Ok(Some(mut report)) => {
                    normalize_spot_order_status_report(&mut report, treat_expired_as_canceled);
                    event_emitter.send_order_status_report(report);
                }
                Ok(None) => log::debug!(
                    "No order status report returned: client_order_id={}",
                    command.client_order_id
                ),
                Err(e) => log::warn!("Failed to query order status: {e}"),
            }

            Ok(())
        });

        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();

        // Spawn instrument bootstrap task
        let http_client = self.http_client.clone();

        get_runtime().spawn(async move {
            match http_client.request_instruments().await {
                Ok(instruments) => {
                    if instruments.is_empty() {
                        log::warn!("No instruments returned for Binance Spot");
                    } else {
                        http_client.cache_instruments(instruments);
                        log::debug!("Instruments initialized");
                    }
                }
                Err(e) => {
                    log::error!("Failed to request Binance Spot instruments: {e}");
                }
            }
        });

        log::info!(
            "Started: client_id={}, account_id={}, account_type={:?}, environment={:?}, product_type={:?}",
            self.core.client_id,
            self.core.account_id,
            self.core.account_type,
            self.config.environment,
            self.config.product_type,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        // Abort WS trading task
        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }

        self.core.set_stopped();
        self.core.set_disconnected();
        self.abort_pending_tasks();
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            log::warn!("generate_order_status_report requires instrument_id: {cmd:?}");
            return Ok(None);
        };

        // Convert ClientOrderId to VenueOrderId if provided (API naming quirk)
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .map(|id| VenueOrderId::new(id.inner()));

        let report = self
            .http_client
            .request_order_status_report(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                cmd.client_order_id,
            )
            .await?;

        Ok(report.map(|mut report| {
            normalize_spot_order_status_report(&mut report, self.config.treat_expired_as_canceled);
            report
        }))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let start_dt = cmd.start.map(|nanos| nanos.to_datetime_utc());
        let end_dt = cmd.end.map(|nanos| nanos.to_datetime_utc());

        let mut reports = self
            .http_client
            .request_order_status_reports(
                self.core.account_id,
                cmd.instrument_id,
                start_dt,
                end_dt,
                cmd.open_only,
                None, // limit
            )
            .await?;

        normalize_spot_order_status_reports(&mut reports, self.config.treat_expired_as_canceled);

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            log::warn!("generate_fill_reports requires instrument_id for Binance Spot");
            return Ok(Vec::new());
        };

        // Convert ClientOrderId to VenueOrderId if provided (API naming quirk)
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .map(|id| VenueOrderId::new(id.inner()));

        let start_dt = cmd.start.map(|nanos| nanos.to_datetime_utc());
        let end_dt = cmd.end.map(|nanos| nanos.to_datetime_utc());

        let reports = self
            .http_client
            .request_fill_reports(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                start_dt,
                end_dt,
                None, // limit
            )
            .await?;

        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        // Spot trading doesn't have positions in the traditional sense
        // Returns empty for spot, could be extended for margin positions
        Ok(Vec::new())
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        log::info!("Generating ExecutionMassStatus (lookback_mins={lookback_mins:?})");

        let ts_now = self.clock.get_time_ns();

        let start = lookback_mins.map(|mins| {
            let lookback_ns = mins_to_nanos(mins);
            UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
        });

        // Binance requires instrument_id for historical orders (open_only=false).
        // Use open_only=true for mass status to get all open orders across instruments.
        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(true)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (order_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        // Note: Fill reports require instrument_id for Binance, so we skip them in mass status
        // They would need to be fetched per-instrument if needed

        log::info!("Received {} OrderStatusReports", order_reports.len());
        log::info!("Received {} PositionReports", position_reports.len());

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *BINANCE_VENUE,
            ts_now,
            None,
        );

        mass_status.add_order_reports(order_reports);
        mass_status.add_position_reports(position_reports);

        Ok(Some(mass_status))
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            let client_order_id = order.client_order_id();
            log::warn!("Cannot submit closed order {client_order_id}");
            return Ok(());
        }

        validate_spot_first_release_order(&order)?;

        log::debug!("OrderSubmitted client_order_id={}", order.client_order_id());
        self.emitter.emit_order_submitted(&order);

        self.submit_order_internal(&cmd)
    }

    fn submit_order_list(&self, _cmd: SubmitOrderList) -> anyhow::Result<()> {
        Err(ExecutionWriteGateError::Unsupported(ExecutionWriteKind::SubmitList).into())
    }

    fn modify_order(&self, _cmd: ModifyOrder) -> anyhow::Result<()> {
        Err(ExecutionWriteGateError::Unsupported(ExecutionWriteKind::Modify).into())
    }

    fn batch_modify_orders(&self, _cmd: BatchModifyOrders) -> anyhow::Result<()> {
        Err(ExecutionWriteGateError::Unsupported(ExecutionWriteKind::BatchModify).into())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        self.cancel_order_internal(&cmd)
    }

    fn cancel_all_orders(&self, _cmd: CancelAllOrders) -> anyhow::Result<()> {
        Err(ExecutionWriteGateError::Unsupported(ExecutionWriteKind::CancelAll).into())
    }

    fn batch_cancel_orders(&self, _cmd: BatchCancelOrders) -> anyhow::Result<()> {
        Err(ExecutionWriteGateError::Unsupported(ExecutionWriteKind::BatchCancel).into())
    }
}

fn validate_spot_first_release_order(order: &OrderAny) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(order.order_type(), OrderType::Market | OrderType::Limit),
        "first-release Binance Spot rejects conditional order type {:?}",
        order.order_type()
    );
    anyhow::ensure!(
        !order.is_quote_quantity(),
        "first-release Binance Spot rejects quote-order quantity"
    );
    anyhow::ensure!(
        order.trigger_price().is_none(),
        "first-release Binance Spot rejects trigger price"
    );
    anyhow::ensure!(
        order.display_qty().is_none(),
        "first-release Binance Spot rejects display quantity"
    );
    Ok(())
}

fn final_time_in_force(
    order_type: OrderType,
    time_in_force: nautilus_model::enums::TimeInForce,
    post_only: bool,
) -> anyhow::Result<ExecutionTimeInForce> {
    if order_type == OrderType::Market {
        return Ok(ExecutionTimeInForce::NotApplicable);
    }
    if post_only {
        return Ok(ExecutionTimeInForce::Gtx);
    }
    match time_in_force {
        nautilus_model::enums::TimeInForce::Gtc => Ok(ExecutionTimeInForce::Gtc),
        nautilus_model::enums::TimeInForce::Ioc => Ok(ExecutionTimeInForce::Ioc),
        nautilus_model::enums::TimeInForce::Fok => Ok(ExecutionTimeInForce::Fok),
        unsupported => {
            anyhow::bail!("first-release Binance Spot rejects time in force {unsupported:?}")
        }
    }
}

fn normalize_spot_order_status_report(
    report: &mut OrderStatusReport,
    treat_expired_as_canceled: bool,
) {
    if treat_expired_as_canceled && report.order_status == OrderStatus::Expired {
        report.order_status = OrderStatus::Canceled;
    }
}

fn normalize_spot_order_status_reports(
    reports: &mut [OrderStatusReport],
    treat_expired_as_canceled: bool,
) {
    for report in reports {
        normalize_spot_order_status_report(report, treat_expired_as_canceled);
    }
}

async fn wait_for_ws_setup_response(
    timeout: Duration,
    success: impl Future<Output = ()>,
    setup_errors: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    timeout_message: &'static str,
) -> anyhow::Result<()> {
    tokio::pin!(success);

    let result = tokio::time::timeout(timeout, async {
        tokio::select! {
            () = &mut success => Ok(()),
            err = setup_errors.recv() => {
                anyhow::bail!(
                    "{}",
                    err.unwrap_or_else(|| "WS setup error channel closed".to_string()),
                )
            }
        }
    })
    .await;

    result.map_err(|_| anyhow::anyhow!(timeout_message))?
}

#[expect(clippy::too_many_arguments)]
fn dispatch_ws_trading_message(
    msg: BinanceSpotWsTradingMessage,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
    ws_authenticated: &tokio::sync::Notify,
    ws_user_data_subscribed: &tokio::sync::Notify,
    ws_setup_error_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
) {
    match msg {
        BinanceSpotWsTradingMessage::OrderAccepted {
            request_id,
            response,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS order accepted: request_id={request_id}, order_id={}",
                response.order_id
            );
            // OrderAccepted event is synthesized from UDS executionReport (New)
        }
        BinanceSpotWsTradingMessage::OrderRejected {
            request_id,
            code,
            msg,
        } => {
            log::debug!("WS order rejected: request_id={request_id}, code={code}, msg={msg}");
            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id) {
                let code_i64 = i64::from(code);
                if matches!(
                    code_i64,
                    BINANCE_UNEXPECTED_RESPONSE_CODE | BINANCE_STATUS_UNKNOWN_CODE
                ) {
                    log::warn!(
                        "Ambiguous WS submit failure for {}, awaiting reconciliation: code={code}, msg={msg}",
                        pending.client_order_id,
                    );
                    return;
                }

                // Clone to drop the DashMap read guard before cleanup_terminal
                let identity = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
                    .map(|r| r.clone());

                if let Some(identity) = identity {
                    let due_post_only = code_i64 == BINANCE_GTX_ORDER_REJECT_CODE
                        || (code_i64 == BINANCE_NEW_ORDER_REJECTED_CODE
                            && msg == BINANCE_SPOT_POST_ONLY_REJECT_MSG);
                    let ts_now = clock.get_time_ns();
                    let rejected = OrderRejected::new(
                        emitter.trader_id(),
                        identity.strategy_id,
                        identity.instrument_id,
                        pending.client_order_id,
                        account_id,
                        Ustr::from(&format!("code={code}: {msg}")),
                        UUID4::new(),
                        ts_now,
                        ts_now,
                        false,
                        due_post_only,
                    );
                    dispatch_state.cleanup_terminal(pending.client_order_id);
                    emitter.send_order_event(OrderEventAny::Rejected(rejected));
                } else {
                    log::warn!(
                        "No order identity for {}, cannot emit OrderRejected",
                        pending.client_order_id
                    );
                }
            } else {
                log::warn!("No pending request for {request_id}, cannot emit OrderRejected");
            }
        }
        BinanceSpotWsTradingMessage::OrderCanceled {
            request_id,
            response,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS order canceled: request_id={request_id}, order_id={}",
                response.order_id
            );
            // OrderCanceled event is synthesized from UDS executionReport (Canceled)
        }
        BinanceSpotWsTradingMessage::CancelRejected {
            request_id,
            code,
            msg,
        } => {
            log::warn!("WS cancel rejected: request_id={request_id}, code={code}, msg={msg}");
            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id)
                && let Some(identity) = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
            {
                let ts_now = clock.get_time_ns();
                let rejected = OrderCancelRejected::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    pending.client_order_id,
                    Ustr::from(&format!("code={code}: {msg}")),
                    UUID4::new(),
                    ts_now,
                    ts_now,
                    false,
                    pending.venue_order_id,
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::CancelRejected(rejected));
            }
        }
        BinanceSpotWsTradingMessage::CancelReplaceAccepted {
            request_id,
            cancel_response,
            new_order_response,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS cancel-replace accepted: request_id={request_id}, \
                 canceled_id={}, new_id={}",
                cancel_response.order_id,
                new_order_response.order_id,
            );
            // OrderUpdated event is synthesized from UDS executionReport (Replaced)
        }
        BinanceSpotWsTradingMessage::CancelReplaceRejected {
            request_id,
            code,
            msg,
        } => {
            log::warn!(
                "WS cancel-replace rejected: request_id={request_id}, code={code}, msg={msg}"
            );

            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id)
                && let Some(identity) = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
            {
                let ts_now = clock.get_time_ns();
                let rejected = OrderModifyRejected::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    pending.client_order_id,
                    Ustr::from(&format!("code={code}: {msg}")),
                    UUID4::new(),
                    ts_now,
                    ts_now,
                    false,
                    pending.venue_order_id,
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::ModifyRejected(rejected));
            }
        }
        BinanceSpotWsTradingMessage::RequestFailed { request_id, msg } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::error!(
                "WS trading request failed without structured venue response: request_id={request_id}, {msg}"
            );
        }
        BinanceSpotWsTradingMessage::AllOrdersCanceled {
            request_id,
            responses,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS all orders canceled: request_id={request_id}, count={}",
                responses.len()
            );
            // Individual OrderCanceled events arrive via UDS executionReport
        }
        BinanceSpotWsTradingMessage::UserDataSubscribed { subscription_id } => {
            log::debug!("User data stream subscribed: id={subscription_id}");
            ws_user_data_subscribed.notify_one();
        }
        BinanceSpotWsTradingMessage::ExecutionReport(report) => {
            let ts_init = clock.get_time_ns();
            dispatch_execution_report(
                &report,
                emitter,
                http_client,
                account_id,
                treat_expired_as_canceled,
                dispatch_state,
                seen_trade_ids,
                ts_init,
            );
        }
        BinanceSpotWsTradingMessage::AccountPosition(position) => {
            let ts_init = clock.get_time_ns();
            let state = parse_spot_account_position(&position, account_id, ts_init);
            emitter.send_account_state(state);
        }
        BinanceSpotWsTradingMessage::BalanceUpdate(update) => {
            log::debug!(
                "Balance update: asset={}, delta={}",
                update.asset,
                update.delta,
            );
            let http_client = http_client.clone();
            let emitter = emitter.clone();

            get_runtime().spawn(async move {
                match http_client.request_account_state(account_id).await {
                    Ok(state) => emitter.send_account_state(state),
                    Err(e) => {
                        log::error!("Failed to refresh account state after balance update: {e}");
                    }
                }
            });
        }
        BinanceSpotWsTradingMessage::Connected => {
            log::debug!("WS trading API connected");
        }
        BinanceSpotWsTradingMessage::Authenticated => {
            log::debug!("WS trading API authenticated");
            ws_authenticated.notify_one();
        }
        BinanceSpotWsTradingMessage::Reconnected => {
            log::info!("WS trading API reconnected");
        }
        BinanceSpotWsTradingMessage::ServerShutdown { event_time } => {
            log::warn!(
                "WS trading API server shutdown notice (event_time={event_time}); reconnect expected within ~10 minutes"
            );
        }
        BinanceSpotWsTradingMessage::Error(err) => {
            log::error!("WS trading API error: {err}");
            let _ = ws_setup_error_tx.send(err);
        }
    }
}

fn build_new_order_params(
    order: &impl Order,
    client_order_id: ClientOrderId,
    is_post_only: bool,
    is_quote_quantity: bool,
) -> anyhow::Result<NewOrderParams> {
    let binance_side = BinanceSide::try_from(order.order_side())?;
    let binance_order_type = order_type_to_binance_spot(order.order_type(), is_post_only)?;

    let requires_trigger = matches!(
        order.order_type(),
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
    );

    if requires_trigger && order.trigger_price().is_none() {
        anyhow::bail!("Conditional orders require a trigger price");
    }

    let supports_tif = matches!(
        binance_order_type,
        BinanceSpotOrderType::Limit
            | BinanceSpotOrderType::StopLossLimit
            | BinanceSpotOrderType::TakeProfitLimit
    );
    let binance_tif = if supports_tif {
        Some(time_in_force_to_binance_spot(order.time_in_force())?)
    } else {
        None
    };

    let qty_str = order.quantity().to_string();
    let (base_qty, quote_qty) = if is_quote_quantity {
        (None, Some(qty_str))
    } else {
        (Some(qty_str), None)
    };

    let client_id_str = encode_broker_id(&client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);

    Ok(NewOrderParams {
        symbol: order.instrument_id().symbol.to_string(),
        side: binance_side,
        order_type: binance_order_type,
        time_in_force: binance_tif,
        quantity: base_qty,
        quote_order_qty: quote_qty,
        price: order.price().map(|p| p.to_string()),
        new_client_order_id: Some(client_id_str),
        stop_price: order.trigger_price().map(|p| p.to_string()),
        trailing_delta: None,
        iceberg_qty: order.display_qty().map(|q| q.to_string()),
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
        strategy_id: None,
        strategy_type: None,
    })
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn build_spot_order_list_params(
    order_list_id: &str,
    orders: &[OrderAny],
) -> Result<NewOcoOrderListParams, String> {
    let has_grouped_order = orders.iter().any(is_grouped_order);

    if has_grouped_order {
        return build_spot_oco_order_list_params(order_list_id, orders);
    }

    Err("Binance Spot order-list submission currently supports only OCO lists".to_string())
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn build_spot_oco_order_list_params(
    order_list_id: &str,
    orders: &[OrderAny],
) -> Result<NewOcoOrderListParams, String> {
    if orders.len() != 2 {
        return Err(format!(
            "Binance Spot OCO order-list submission requires exactly 2 orders, was {}",
            orders.len()
        ));
    }

    if orders
        .iter()
        .any(|order| order.contingency_type() != Some(ContingencyType::Oco))
    {
        return Err(
            "Binance Spot grouped order-list submission currently supports only OCO lists"
                .to_string(),
        );
    }

    let first = &orders[0];
    let second = &orders[1];
    if first.instrument_id() != second.instrument_id() {
        return Err("Binance Spot OCO order-list legs must use the same instrument".to_string());
    }

    if first.order_side() != second.order_side() {
        return Err("Binance Spot OCO order-list legs must use the same side".to_string());
    }

    if first.quantity() != second.quantity() {
        return Err("Binance Spot OCO order-list legs must use the same quantity".to_string());
    }

    if first.is_quote_quantity() || second.is_quote_quantity() {
        return Err("Binance Spot OCO order-list legs do not support quote quantity".to_string());
    }

    let mut above = None;
    let mut below = None;

    for order in orders {
        let params =
            build_new_order_params(order, order.client_order_id(), order.is_post_only(), false)
                .map_err(|e| e.to_string())?;

        match spot_oco_leg_position(params.side, params.order_type)? {
            SpotOcoLegPosition::Above => {
                if above.replace(params).is_some() {
                    return Err(
                        "Binance Spot OCO order-list resolved more than one above leg".to_string(),
                    );
                }
            }
            SpotOcoLegPosition::Below => {
                if below.replace(params).is_some() {
                    return Err(
                        "Binance Spot OCO order-list resolved more than one below leg".to_string(),
                    );
                }
            }
        }
    }

    let above = above.ok_or_else(|| "Binance Spot OCO order-list missing above leg".to_string())?;
    let below = below.ok_or_else(|| "Binance Spot OCO order-list missing below leg".to_string())?;
    let quantity = above
        .quantity
        .clone()
        .ok_or_else(|| "Binance Spot OCO order-list requires base quantity".to_string())?;

    Ok(NewOcoOrderListParams {
        symbol: first.instrument_id().symbol.to_string(),
        list_client_order_id: Some(order_list_id.to_string()),
        side: above.side,
        quantity,
        above_type: above.order_type,
        above_client_order_id: above.new_client_order_id,
        above_iceberg_qty: above.iceberg_qty,
        above_price: above.price,
        above_stop_price: above.stop_price,
        above_time_in_force: above.time_in_force,
        below_type: below.order_type,
        below_client_order_id: below.new_client_order_id,
        below_iceberg_qty: below.iceberg_qty,
        below_price: below.price,
        below_stop_price: below.stop_price,
        below_time_in_force: below.time_in_force,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    })
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
enum SpotOcoLegPosition {
    Above,
    Below,
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn spot_oco_leg_position(
    side: BinanceSide,
    order_type: BinanceSpotOrderType,
) -> Result<SpotOcoLegPosition, String> {
    match (side, order_type) {
        (
            BinanceSide::Sell,
            BinanceSpotOrderType::LimitMaker
            | BinanceSpotOrderType::TakeProfit
            | BinanceSpotOrderType::TakeProfitLimit,
        )
        | (
            BinanceSide::Buy,
            BinanceSpotOrderType::StopLoss | BinanceSpotOrderType::StopLossLimit,
        ) => Ok(SpotOcoLegPosition::Above),
        (
            BinanceSide::Sell,
            BinanceSpotOrderType::StopLoss | BinanceSpotOrderType::StopLossLimit,
        )
        | (
            BinanceSide::Buy,
            BinanceSpotOrderType::LimitMaker
            | BinanceSpotOrderType::TakeProfit
            | BinanceSpotOrderType::TakeProfitLimit,
        ) => Ok(SpotOcoLegPosition::Below),
        (_, unsupported) => Err(format!(
            "Unsupported Binance Spot OCO leg order type: {unsupported:?}"
        )),
    }
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn is_grouped_order(order: &OrderAny) -> bool {
    matches!(
        order.contingency_type(),
        Some(contingency_type) if contingency_type != ContingencyType::NoContingency
    ) || order
        .linked_order_ids()
        .is_some_and(|linked_order_ids| !linked_order_ids.is_empty())
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn handle_spot_order_list_submit_error(
    event_emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    trader_id: TraderId,
    account_id: AccountId,
    clock: &'static AtomicTime,
    orders: &[OrderAny],
    error: BinanceSpotHttpError,
) -> anyhow::Result<()> {
    let ambiguous = matches!(
        error,
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_UNEXPECTED_RESPONSE_CODE | BINANCE_STATUS_UNKNOWN_CODE,
            ..
        }
    );

    if ambiguous {
        log::error!("Ambiguous order-list submit failure, awaiting reconciliation: {error}");
        return Err(error.into());
    }

    let reject_orders = matches!(
        error,
        BinanceSpotHttpError::BinanceError { .. }
            | BinanceSpotHttpError::MissingCredentials
            | BinanceSpotHttpError::ValidationError(_)
    );

    if reject_orders {
        let ts_now = clock.get_time_ns();
        let reason = format!("submit-order-list-error: {error}");
        for order in orders {
            let client_order_id = order.client_order_id();
            dispatch_state.cleanup_terminal(client_order_id);
            let rejected = OrderRejected::new(
                trader_id,
                order.strategy_id(),
                order.instrument_id(),
                client_order_id,
                account_id,
                reason.clone().into(),
                UUID4::new(),
                ts_now,
                ts_now,
                false,
                false,
            );
            event_emitter.send_order_event(OrderEventAny::Rejected(rejected));
        }
    } else {
        log::error!("Order-list submit failed, awaiting reconciliation: {error}");
    }

    Err(error.into())
}

fn build_cancel_order_params(cmd: &CancelOrder) -> CancelOrderParams {
    let order_id = cmd
        .venue_order_id
        .and_then(|id| id.inner().parse::<i64>().ok());

    if let Some(order_id) = order_id {
        CancelOrderParams::by_order_id(cmd.instrument_id.symbol.to_string(), order_id)
    } else {
        let client_id_str = encode_broker_id(&cmd.client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        CancelOrderParams::by_client_order_id(cmd.instrument_id.symbol.to_string(), client_id_str)
    }
}

#[allow(
    dead_code,
    reason = "retained for deny-only serializer inventory tests"
)]
fn build_cancel_replace_params(
    cmd: &ModifyOrder,
    order: &impl Order,
    quantity: Quantity,
) -> anyhow::Result<CancelReplaceOrderParams> {
    let binance_side = BinanceSide::try_from(order.order_side())?;
    let binance_order_type = order_type_to_binance_spot(order.order_type(), false)?;
    let binance_tif = time_in_force_to_binance_spot(order.time_in_force())?;

    let cancel_order_id: Option<i64> = cmd
        .venue_order_id
        .map(|id| {
            id.inner()
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Invalid venue order ID: {id}"))
        })
        .transpose()?;

    let client_id_str = encode_broker_id(&cmd.client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);

    Ok(CancelReplaceOrderParams {
        symbol: cmd.instrument_id.symbol.to_string(),
        side: binance_side,
        order_type: binance_order_type,
        cancel_replace_mode: BinanceCancelReplaceMode::StopOnFailure,
        time_in_force: Some(binance_tif),
        quantity: Some(quantity.to_string()),
        quote_order_qty: None,
        price: cmd.price.map(|p| p.to_string()),
        cancel_order_id,
        cancel_orig_client_order_id: if cancel_order_id.is_none() {
            Some(client_id_str.clone())
        } else {
            None
        },
        new_client_order_id: Some(client_id_str),
        stop_price: None,
        trailing_delta: None,
        iceberg_qty: None,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    })
}

/// Dispatches a Spot execution report with tracked/untracked routing.
///
/// Tracked orders (with registered identity) produce proper order events.
/// Untracked orders fall back to execution reports for reconciliation.
#[expect(clippy::too_many_arguments)]
fn dispatch_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    dispatch_state: &WsDispatchState,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    ts_init: UnixNanos,
) {
    let symbol = report.symbol;
    let instrument_id = InstrumentId::new(symbol.into(), *BINANCE_VENUE);
    let (price_precision, size_precision) = http_client
        .get_instrument(&symbol)
        .map_or((8, 8), |i| (i.price_precision(), i.size_precision()));

    let client_order_id = ClientOrderId::new(decode_broker_id(
        &report.client_order_id,
        BINANCE_NAUTILUS_SPOT_BROKER_ID,
    ));

    let identity = dispatch_state
        .order_identities
        .get(&client_order_id)
        .map(|r| r.clone());

    if let Some(identity) = identity {
        dispatch_tracked_execution_report(
            report,
            emitter,
            account_id,
            treat_expired_as_canceled,
            dispatch_state,
            seen_trade_ids,
            client_order_id,
            &identity,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    } else {
        dispatch_untracked_execution_report(
            report,
            emitter,
            http_client,
            account_id,
            treat_expired_as_canceled,
            seen_trade_ids,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    }
}

/// Dispatches a tracked execution report as proper order events.
#[expect(clippy::too_many_arguments)]
fn dispatch_tracked_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    state: &WsDispatchState,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    client_order_id: ClientOrderId,
    identity: &OrderIdentity,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) {
    let venue_order_id = VenueOrderId::new(report.order_id.to_string());
    let ts_event = UnixNanos::from_millis(report.event_time as u64);

    match report.execution_type {
        BinanceSpotExecutionType::New => {
            if state.has_filled(&client_order_id) {
                log::debug!("Skipping New for already-filled {client_order_id}");
                return;
            }

            if state.has_emitted_accepted(&client_order_id) {
                // Already accepted: this New is a cancel-replace result
                let Some(price) = parse_spot_execution_report_price(
                    report,
                    &report.price,
                    price_precision,
                    "price",
                ) else {
                    return;
                };
                let Some(quantity) = parse_spot_execution_report_quantity(
                    report,
                    &report.original_qty,
                    size_precision,
                    "original_qty",
                ) else {
                    return;
                };
                let Some(stop_price) =
                    parse_spot_execution_report_decimal(report, &report.stop_price, "stop_price")
                else {
                    return;
                };
                let trigger = if stop_price > Decimal::ZERO {
                    let Some(trigger_price) = parse_spot_execution_report_price(
                        report,
                        &report.stop_price,
                        price_precision,
                        "stop_price",
                    ) else {
                        return;
                    };
                    Some(trigger_price)
                } else {
                    None
                };
                let updated = OrderUpdated::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    client_order_id,
                    quantity,
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    Some(venue_order_id),
                    Some(account_id),
                    Some(price),
                    trigger,
                    None,  // protection_price
                    false, // is_quote_quantity
                );
                emitter.send_order_event(OrderEventAny::Updated(updated));
                return;
            }
            state.insert_accepted(client_order_id);
            let accepted = OrderAccepted::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                venue_order_id,
                account_id,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
            );
            emitter.send_order_event(OrderEventAny::Accepted(accepted));
        }
        BinanceSpotExecutionType::Trade => {
            let dedup_key = (report.symbol, report.trade_id);
            let mut guard = seen_trade_ids.lock().expect(MUTEX_POISONED);
            let is_duplicate = guard.contains(&dedup_key);
            guard.add(dedup_key);
            drop(guard);

            if is_duplicate {
                log::debug!(
                    "Duplicate trade_id={} for {}, skipping",
                    report.trade_id,
                    report.symbol
                );
                return;
            }

            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );

            let Some(last_qty) = parse_spot_execution_report_quantity(
                report,
                &report.last_filled_qty,
                size_precision,
                "last_filled_qty",
            ) else {
                return;
            };
            let Some(last_px) = parse_spot_execution_report_price(
                report,
                &report.last_filled_price,
                price_precision,
                "last_filled_price",
            ) else {
                return;
            };
            let Some(commission) =
                parse_spot_execution_report_decimal(report, &report.commission, "commission")
            else {
                return;
            };
            let commission_currency = report
                .commission_asset
                .as_ref()
                .map_or_else(Currency::USDT, |a| {
                    Currency::get_or_create_crypto(a.as_str())
                });
            let commission_money = match Money::from_decimal(commission, commission_currency) {
                Ok(money) => money,
                Err(e) => {
                    log::warn!(
                        "Failed to build Spot commission money for symbol={}, order_id={}, \
                        trade_id={}: {e}",
                        report.symbol,
                        report.order_id,
                        report.trade_id,
                    );
                    return;
                }
            };

            let liquidity_side = if report.is_maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            };

            let filled = OrderFilled::new(
                emitter.trader_id(),
                identity.strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                account_id,
                TradeId::new(report.trade_id.to_string()),
                identity.order_side,
                identity.order_type,
                last_qty,
                last_px,
                commission_currency,
                liquidity_side,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                None,
                Some(commission_money),
            );

            state.insert_filled(client_order_id);
            emitter.send_order_event(OrderEventAny::Filled(filled));

            let cumulative_qty = parse_spot_execution_report_decimal(
                report,
                &report.cumulative_filled_qty,
                "cumulative_filled_qty",
            );
            let original_qty =
                parse_spot_execution_report_decimal(report, &report.original_qty, "original_qty");
            if let (Some(original_qty), Some(cumulative_qty)) = (original_qty, cumulative_qty)
                && original_qty <= cumulative_qty
            {
                state.cleanup_terminal(client_order_id);
            }
        }
        BinanceSpotExecutionType::Replaced => {
            // Cancel-replace succeeded: the old order is being replaced.
            // The replacement NEW event follows with the new price/qty.
            log::debug!(
                "Order replaced: client_order_id={client_order_id}, venue_order_id={venue_order_id}"
            );
        }
        BinanceSpotExecutionType::Canceled | BinanceSpotExecutionType::TradePrevention => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            let canceled = OrderCanceled::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                Some(venue_order_id),
                Some(account_id),
            );
            state.cleanup_terminal(client_order_id);
            emitter.send_order_event(OrderEventAny::Canceled(canceled));
        }
        BinanceSpotExecutionType::Expired => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            state.cleanup_terminal(client_order_id);

            if treat_expired_as_canceled {
                let canceled = OrderCanceled::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    client_order_id,
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    Some(venue_order_id),
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::Canceled(canceled));
            } else {
                let expired = OrderExpired::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    client_order_id,
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    Some(venue_order_id),
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::Expired(expired));
            }
        }
        BinanceSpotExecutionType::Rejected => {
            let reason = if report.reject_reason.is_empty() {
                Ustr::from("Order rejected by venue")
            } else {
                Ustr::from(&report.reject_reason)
            };
            let due_post_only = report.time_in_force == BinanceTimeInForce::Gtx
                || (report.order_type == "LIMIT_MAKER"
                    && (report.reject_reason.is_empty() || report.reject_reason == "NONE"));
            state.cleanup_terminal(client_order_id);
            emitter.emit_order_rejected_event(
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                reason.as_str(),
                ts_init,
                due_post_only,
            );
        }
    }
}

fn parse_spot_execution_report_quantity(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    precision: u8,
    field: &str,
) -> Option<Quantity> {
    match parse_required_quantity_at_precision(raw, precision, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn parse_spot_execution_report_price(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    precision: u8,
    field: &str,
) -> Option<Price> {
    match parse_required_price_at_precision(raw, precision, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn parse_spot_execution_report_decimal(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    field: &str,
) -> Option<Decimal> {
    match parse_required_decimal(raw, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn warn_invalid_spot_execution_report_field(
    report: &BinanceSpotExecutionReport,
    field: &str,
    error: &anyhow::Error,
) {
    log::warn!(
        "Failed to parse Spot execution report {field} for symbol={}, order_id={}, \
        trade_id={}, client_order_id={}: {error}",
        report.symbol,
        report.order_id,
        report.trade_id,
        report.client_order_id,
    );
}

/// Dispatches an untracked execution report as execution reports for reconciliation.
#[expect(clippy::too_many_arguments)]
fn dispatch_untracked_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    _http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) {
    match report.execution_type {
        BinanceSpotExecutionType::Trade => {
            let dedup_key = (report.symbol, report.trade_id);
            let mut guard = seen_trade_ids.lock().expect(MUTEX_POISONED);
            let is_duplicate = guard.contains(&dedup_key);
            guard.add(dedup_key);
            drop(guard);

            if is_duplicate {
                log::debug!(
                    "Duplicate trade_id={} for {}, skipping",
                    report.trade_id,
                    report.symbol
                );
                return;
            }

            match parse_spot_exec_report_to_order_status(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                treat_expired_as_canceled,
                ts_init,
            ) {
                Ok(status) => emitter.send_order_status_report(status),
                Err(e) => log::error!("Failed to parse order status report: {e}"),
            }

            match parse_spot_exec_report_to_fill(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                ts_init,
            ) {
                Ok(fill) => emitter.send_fill_report(fill),
                Err(e) => log::error!("Failed to parse fill report: {e}"),
            }
        }
        BinanceSpotExecutionType::New
        | BinanceSpotExecutionType::Canceled
        | BinanceSpotExecutionType::Replaced
        | BinanceSpotExecutionType::Rejected
        | BinanceSpotExecutionType::Expired
        | BinanceSpotExecutionType::TradePrevention => {
            match parse_spot_exec_report_to_order_status(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                treat_expired_as_canceled,
                ts_init,
            ) {
                Ok(status) => emitter.send_order_status_report(status),
                Err(e) => log::error!("Failed to parse order status report: {e}"),
            }
        }
    }
}

// Checks for GTX (-5022) and spot LIMIT_MAKER (-2010 + specific message)
fn is_spot_post_only_rejection(error: &BinanceSpotHttpError) -> bool {
    match error {
        BinanceSpotHttpError::BinanceError { code, message } => {
            *code == BINANCE_GTX_ORDER_REJECT_CODE
                || (*code == BINANCE_NEW_ORDER_REJECTED_CODE
                    && message == BINANCE_SPOT_POST_ONLY_REJECT_MSG)
        }
        _ => false,
    }
}

fn is_structured_venue_rejection(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(|be| matches!(be, BinanceSpotHttpError::BinanceError { .. }))
}

fn is_ambiguous_submit_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(|be| {
            matches!(
                be,
                BinanceSpotHttpError::BinanceError {
                    code: BINANCE_UNEXPECTED_RESPONSE_CODE | BINANCE_STATUS_UNKNOWN_CODE,
                    ..
                }
            )
        })
}

fn is_local_command_failure(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(is_local_http_command_failure)
}

fn is_local_http_command_failure(err: &BinanceSpotHttpError) -> bool {
    matches!(
        err,
        BinanceSpotHttpError::MissingCredentials | BinanceSpotHttpError::ValidationError(_)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nautilus_common::messages::ExecutionEvent;
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_model::{
        enums::{AccountType, LiquiditySide, OrderSide},
        identifiers::{StrategyId, TraderId},
    };
    use rstest::rstest;

    use super::*;
    use crate::common::enums::{BinanceEnvironment, BinanceSelfTradePreventionMode};

    fn serialized_field_names<T: serde::Serialize>(value: &T) -> BTreeSet<String> {
        serde_urlencoded::to_string(value)
            .unwrap()
            .split('&')
            .map(|pair| pair.split_once('=').unwrap().0.to_string())
            .collect()
    }

    #[test]
    fn final_spot_unsigned_request_field_inventory_is_exhaustive() {
        let submit = NewOrderParams {
            symbol: "BTCUSDT".to_string(),
            side: BinanceSide::Buy,
            order_type: BinanceSpotOrderType::Limit,
            time_in_force: Some(BinanceTimeInForce::Gtc),
            quantity: Some("0.001".to_string()),
            quote_order_qty: Some("50".to_string()),
            price: Some("50000".to_string()),
            new_client_order_id: Some("SAE-SPOT-1".to_string()),
            stop_price: Some("49000".to_string()),
            trailing_delta: Some(10),
            iceberg_qty: Some("0.0001".to_string()),
            new_order_resp_type: Some(BinanceOrderResponseType::Full),
            self_trade_prevention_mode: Some(BinanceSelfTradePreventionMode::ExpireMaker),
            strategy_id: Some(1),
            strategy_type: Some(1_000_000),
        };
        assert_eq!(
            serialized_field_names(&submit),
            [
                "symbol",
                "side",
                "type",
                "timeInForce",
                "quantity",
                "quoteOrderQty",
                "price",
                "newClientOrderId",
                "stopPrice",
                "trailingDelta",
                "icebergQty",
                "newOrderRespType",
                "selfTradePreventionMode",
                "strategyId",
                "strategyType",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let cancel = CancelOrderParams {
            symbol: "BTCUSDT".to_string(),
            order_id: Some(1),
            orig_client_order_id: Some("SAE-SPOT-1".to_string()),
            new_client_order_id: Some("CANCEL-REPLACEMENT".to_string()),
        };
        assert_eq!(
            serialized_field_names(&cancel),
            ["symbol", "orderId", "origClientOrderId", "newClientOrderId"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_emits_cancel_rejected_and_clears_pending_request() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("TEST"),
            InstrumentId::from("BTCUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-cancel".to_string(),
            PendingRequest {
                client_order_id: ClientOrderId::from("TEST"),
                venue_order_id: Some(VenueOrderId::from("12345")),
                operation: PendingOperation::Cancel,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::CancelRejected {
                request_id: "req-cancel".to_string(),
                code: -2011,
                msg: "Unknown order sent".to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-cancel").is_none());

        match rx
            .try_recv()
            .expect("Cancel rejection event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) => {
                assert_eq!(event.client_order_id, ClientOrderId::from("TEST"));
                assert_eq!(event.account_id, Some(AccountId::from("BINANCE-001")));
                assert!(event.reason.as_str().contains("code=-2011"));
            }
            other => panic!("Expected CancelRejected event, was {other:?}"),
        }
    }

    #[rstest]
    #[case(
        BINANCE_UNEXPECTED_RESPONSE_CODE,
        "An unexpected response was received from the message bus"
    )]
    #[case(
        BINANCE_STATUS_UNKNOWN_CODE,
        "Timeout waiting for response from backend server"
    )]
    fn test_dispatch_ws_trading_message_unknown_status_keeps_order_registered(
        #[case] code: i64,
        #[case] msg: &str,
    ) {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("TEST");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-submit".to_string(),
            PendingRequest {
                client_order_id,
                venue_order_id: None,
                operation: PendingOperation::Place,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::OrderRejected {
                request_id: "req-submit".to_string(),
                code: code as i32,
                msg: msg.to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-submit").is_none());
        assert!(
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .is_some()
        );
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_definite_submit_rejection_emits_order_rejected() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("TEST");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-submit".to_string(),
            PendingRequest {
                client_order_id,
                venue_order_id: None,
                operation: PendingOperation::Place,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::OrderRejected {
                request_id: "req-submit".to_string(),
                code: BINANCE_NEW_ORDER_REJECTED_CODE as i32,
                msg: BINANCE_SPOT_POST_ONLY_REJECT_MSG.to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-submit").is_none());
        assert!(
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .is_none()
        );

        match rx
            .try_recv()
            .expect("OrderRejected event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
                assert!(event.reason.as_str().contains("code=-2010"));
                assert!(event.due_post_only);
            }
            other => panic!("Expected OrderRejected event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_emits_modify_rejected_and_clears_pending_request() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("TEST"),
            InstrumentId::from("BTCUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-modify".to_string(),
            PendingRequest {
                client_order_id: ClientOrderId::from("TEST"),
                venue_order_id: Some(VenueOrderId::from("12345")),
                operation: PendingOperation::Modify,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::CancelReplaceRejected {
                request_id: "req-modify".to_string(),
                code: -2021,
                msg: "Order cancel-replace partially failed".to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-modify").is_none());

        match rx
            .try_recv()
            .expect("Modify rejection event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::ModifyRejected(event)) => {
                assert_eq!(event.client_order_id, ClientOrderId::from("TEST"));
                assert_eq!(event.account_id, Some(AccountId::from("BINANCE-001")));
                assert!(event.reason.as_str().contains("code=-2021"));
            }
            other => panic!("Expected ModifyRejected event, was {other:?}"),
        }
    }

    fn create_test_emitter(
        clock: &'static AtomicTime,
    ) -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TESTER-001"),
            AccountId::from("BINANCE-001"),
            AccountType::Cash,
            None,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(tx);
        (emitter, rx)
    }

    fn create_test_http_client(clock: &'static AtomicTime) -> BinanceSpotHttpClient {
        BinanceSpotHttpClient::new(
            BinanceEnvironment::Live,
            clock,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Test HTTP client should be created")
    }

    fn create_tracked_dispatch_state(
        client_order_id: ClientOrderId,
        instrument_id: InstrumentId,
    ) -> WsDispatchState {
        let dispatch_state = WsDispatchState::default();
        dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id: StrategyId::from("TEST-STRATEGY"),
                order_side: OrderSide::Buy,
                order_type: OrderType::Limit,
                price: None,
                quantity: Quantity::from("1"),
            },
        );
        dispatch_state
    }

    #[rstest]
    #[case::gtx(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_GTX_ORDER_REJECT_CODE,
            message: "Order would immediately trigger.".to_string(),
        },
        true,
    )]
    #[case::spot_post_only(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_NEW_ORDER_REJECTED_CODE,
            message: BINANCE_SPOT_POST_ONLY_REJECT_MSG.to_string(),
        },
        true,
    )]
    #[case::new_order_rejected_other_message(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_NEW_ORDER_REJECTED_CODE,
            message: "Insufficient balance.".to_string(),
        },
        false,
    )]
    #[case::unrelated_code(
        BinanceSpotHttpError::BinanceError {
            code: -2011,
            message: "Unknown order sent.".to_string(),
        },
        false,
    )]
    #[case::non_binance_error(
        BinanceSpotHttpError::NetworkError("connection reset".to_string()),
        false,
    )]
    fn test_is_spot_post_only_rejection(
        #[case] error: BinanceSpotHttpError,
        #[case] expected: bool,
    ) {
        assert_eq!(is_spot_post_only_rejection(&error), expected);
    }

    #[rstest]
    #[case(BINANCE_UNEXPECTED_RESPONSE_CODE)]
    #[case(BINANCE_STATUS_UNKNOWN_CODE)]
    fn test_unknown_status_submit_error_is_ambiguous(#[case] code: i64) {
        let err = anyhow::Error::new(BinanceSpotHttpError::BinanceError {
            code,
            message: "test error".to_string(),
        });
        assert!(is_ambiguous_submit_error(&err));
        assert!(is_structured_venue_rejection(&err));
    }

    #[rstest]
    fn test_other_structured_submit_error_is_not_ambiguous() {
        let err = anyhow::Error::new(BinanceSpotHttpError::BinanceError {
            code: BINANCE_GTX_ORDER_REJECT_CODE,
            message: "test error".to_string(),
        });
        assert!(!is_ambiguous_submit_error(&err));
        assert!(is_structured_venue_rejection(&err));
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_trade_dedup() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("x-TD67BGP9-T0000000000000");
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("O-20200101-000000-000-000-0"),
            InstrumentId::from("ETHUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let trade_json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_trade.json",
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&trade_json).unwrap();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report.clone())),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );
        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        let fills: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))))
            .collect();
        assert_eq!(fills.len(), 1, "duplicate trade should be deduped");

        match fills[0] {
            ExecutionEvent::Order(OrderEventAny::Filled(fill)) => {
                assert_eq!(
                    fill.client_order_id,
                    ClientOrderId::from("O-20200101-000000-000-000-0"),
                );
                assert_eq!(fill.trade_id, TradeId::new("98765432"));
                assert_eq!(fill.liquidity_side, LiquiditySide::Maker);
            }
            _ => unreachable!(),
        }
        let _ = client_order_id;
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_invalid_fill_qty_skips_filled_event() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("O-20200101-000000-000-000-0"),
            InstrumentId::from("ETHUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let trade_json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_trade.json",
        );
        let mut report: BinanceSpotExecutionReport = serde_json::from_str(&trade_json).unwrap();
        report.last_filled_qty = "not-a-number".to_string();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        assert!(
            events
                .iter()
                .all(|e| !matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_)))),
            "invalid fill quantity must not emit OrderFilled",
        );
    }

    #[rstest]
    #[case::as_expired(false, OrderStatus::Expired)]
    #[case::as_canceled(true, OrderStatus::Canceled)]
    fn test_normalize_spot_order_status_report_expired_respects_config(
        #[case] treat_expired_as_canceled: bool,
        #[case] expected: OrderStatus,
    ) {
        let clock = get_atomic_clock_realtime();
        let json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_expired.json",
        );
        let msg: BinanceSpotExecutionReport = serde_json::from_str(&json).unwrap();
        let mut report = parse_spot_exec_report_to_order_status(
            &msg,
            InstrumentId::from("ETHUSDT.BINANCE"),
            2,
            5,
            AccountId::from("BINANCE-001"),
            false,
            clock.get_time_ns(),
        )
        .unwrap();
        let mut reports = vec![report.clone()];

        normalize_spot_order_status_report(&mut report, treat_expired_as_canceled);
        normalize_spot_order_status_reports(&mut reports, treat_expired_as_canceled);

        assert_eq!(report.order_status, expected);
        assert_eq!(reports[0].order_status, expected);
    }

    #[rstest]
    #[case::as_expired(false)]
    #[case::as_canceled(true)]
    fn test_dispatch_tracked_execution_report_expired_respects_config(
        #[case] treat_expired_as_canceled: bool,
    ) {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let client_order_id = ClientOrderId::from("O-20200101-000000-000-000-0");
        let instrument_id = InstrumentId::from("ETHUSDT.BINANCE");
        let dispatch_state = WsDispatchState::default();
        dispatch_state.insert_accepted(client_order_id);
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));
        let identity = OrderIdentity {
            instrument_id,
            strategy_id: StrategyId::from("TEST-STRATEGY"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            price: None,
            quantity: Quantity::from("1"),
        };

        let json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_expired.json",
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&json).unwrap();

        dispatch_tracked_execution_report(
            &report,
            &emitter,
            AccountId::from("BINANCE-001"),
            treat_expired_as_canceled,
            &dispatch_state,
            &seen_trade_ids,
            client_order_id,
            &identity,
            instrument_id,
            2,
            5,
            clock.get_time_ns(),
        );

        let event = rx.try_recv().expect("terminal order event expected");
        match (treat_expired_as_canceled, event) {
            (true, ExecutionEvent::Order(OrderEventAny::Canceled(event))) => {
                assert_eq!(event.client_order_id, client_order_id);
            }
            (false, ExecutionEvent::Order(OrderEventAny::Expired(event))) => {
                assert_eq!(event.client_order_id, client_order_id);
            }
            (_, other) => panic!("Expected terminal expired/canceled event, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_rejected_gtx_sets_post_only() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("O-20200101-000000-000-000-1");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("ETHUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let encoded = encode_broker_id(&client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        let report_json = format!(
            r#"{{
                "e":"executionReport","E":1709654400000,"s":"ETHUSDT",
                "c":"{encoded}","S":"BUY","o":"LIMIT","f":"GTX",
                "q":"1.00000000","p":"2500.00000000","P":"0.00000000",
                "x":"REJECTED","X":"REJECTED","r":"NONE","i":12345678,
                "l":"0.00000000","z":"0.00000000","L":"0.00000000",
                "n":"0","N":null,"T":1709654400000,"t":-1,"w":false,"m":false,
                "O":1709654400000,"Z":"0.00000000","C":""
            }}"#,
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&report_json).unwrap();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );

        match rx.try_recv().expect("OrderRejected event expected") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
                assert!(event.due_post_only);
            }
            other => panic!("Expected OrderRejected event, was {other:?}"),
        }
    }
}
