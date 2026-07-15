// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Final execution-write authority boundary shared by live venue adapters.
//!
//! The types in this module describe the complete native request immediately before transport.
//! They deliberately contain no application authority or writer-lease type. Applications inject
//! an [`ExecutionWriteGate`] which canonicalizes the view, consumes the exact registered command,
//! and returns an owned permit retained through the transport outcome.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use nautilus_core::UUID4;
use nautilus_model::{
    enums::{OrderSide, OrderType},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, VenueOrderId},
    types::{Money, Price, Quantity},
};
use serde::{Deserialize, Serialize};

pub use crate::execution_persistence::ExecutionWriteKind;

/// Opaque digest of the complete canonical final-write payload.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FinalWritePayloadDigest(pub [u8; 32]);

/// Opaque digest of the lean execution-write context.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriteContextDigest(pub [u8; 32]);

/// Binance product selected by the execution client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionProduct {
    /// Binance Spot.
    Spot,
    /// Binance USD-M or COIN-M Futures.
    Futures,
}

/// Exact route selected for a final write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRoute {
    /// Binance Spot route.
    BinanceSpot,
    /// Binance Futures route.
    BinanceFutures,
}

/// HTTP method selected for a final request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionHttpMethod {
    /// HTTP POST.
    Post,
    /// HTTP DELETE.
    Delete,
}

/// First-release HTTP endpoint selected for a final request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEndpointPath {
    /// `/api/v3/order`.
    SpotOrder,
    /// `/fapi/v1/order` or `/dapi/v1/order` selected by the Futures product client.
    FuturesOrder,
}

/// Final transport target before dynamic authentication fields are added.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport_kind", rename_all = "snake_case")]
pub enum ExecutionTransportTargetViewV1 {
    /// Signed HTTP request.
    Http {
        /// Exact method.
        method: ExecutionHttpMethod,
        /// Exact endpoint.
        path: ExecutionEndpointPath,
        /// Reviewed receive window in milliseconds.
        recv_window_ms: u64,
    },
    /// Authenticated WebSocket API order transport.
    WebSocketApi {
        /// Exact WebSocket API method.
        method: String,
        /// Reviewed receive window in milliseconds.
        recv_window_ms: u64,
    },
}

/// Binance position-side control represented without coupling common to the adapter crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPositionSide {
    /// Long hedge-mode side.
    Long,
    /// Short hedge-mode side.
    Short,
    /// One-way position side.
    Both,
}

/// Binance trigger-price source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWorkingType {
    /// Mark price.
    MarkPrice,
    /// Contract price.
    ContractPrice,
}

/// Binance order response commitment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOrderResponseMode {
    /// Spot `FULL` response.
    SpotFull,
    /// Futures response type omitted.
    FuturesOmitted,
}

/// Canonicalized time-in-force semantics in the final unsigned request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTimeInForce {
    /// Good till canceled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
    /// Post-only good till crossing prevention.
    Gtx,
    /// Field omitted for a market order.
    NotApplicable,
}

/// One ordered native element in a final unsigned request.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "element_kind", rename_all = "snake_case")]
pub enum ExecutionWriteElementViewV1 {
    /// Submit one order.
    Submit {
        /// Zero-based contiguous batch position.
        batch_index: u16,
        /// Exact instrument.
        instrument_id: InstrumentId,
        /// Exact economic client order ID.
        client_order_id: ClientOrderId,
        /// Exact order side.
        side: OrderSide,
        /// Exact native order type.
        order_type: OrderType,
        /// Exact final time-in-force semantics.
        time_in_force: ExecutionTimeInForce,
        /// Base quantity.
        quantity: Quantity,
        /// Optional limit price.
        price: Option<Price>,
        /// Unsupported trigger price, required to remain absent.
        trigger_price: Option<Price>,
        /// Unsupported quote quantity, required to remain absent.
        quote_quantity: Option<Money>,
        /// Unsupported display quantity, required to remain absent.
        display_quantity: Option<Quantity>,
        /// Exact Futures reduce-only bit.
        reduce_only: bool,
        /// Exact post-only bit.
        post_only: bool,
        /// Unsupported hedge-mode side, required to remain absent.
        position_side: Option<ExecutionPositionSide>,
        /// Unsupported close-position bit, required to remain absent.
        close_position: Option<bool>,
        /// Unsupported trigger-price source, required to remain absent.
        working_type: Option<ExecutionWorkingType>,
        /// Unsupported price-protection bit, required to remain absent.
        price_protect: Option<bool>,
        /// Exact route-specific response commitment.
        response_mode: ExecutionOrderResponseMode,
        /// Unsupported good-till-date, required to remain absent.
        good_till_date_ms: Option<i64>,
        /// Unsupported self-trade prevention mode, required to remain absent.
        self_trade_prevention_mode: Option<String>,
        /// Unsupported trailing delta, required to remain absent.
        trailing_delta: Option<i64>,
        /// Unsupported activation price, required to remain absent.
        activation_price: Option<Price>,
        /// Unsupported callback rate, required to remain absent.
        callback_rate: Option<String>,
        /// Unsupported price-match mode, required to remain absent.
        price_match: Option<String>,
        /// Unsupported Spot strategy ID, required to remain absent.
        strategy_id: Option<i64>,
        /// Unsupported Spot strategy type, required to remain absent.
        strategy_type: Option<i64>,
    },
    /// Modify one order, represented only for typed denial.
    Modify {
        /// Zero-based contiguous batch position.
        batch_index: u16,
        /// Exact instrument.
        instrument_id: InstrumentId,
        /// Exact client order ID.
        client_order_id: ClientOrderId,
        /// Exact side.
        side: OrderSide,
        /// Replacement quantity.
        quantity: Quantity,
        /// Replacement price.
        price: Price,
    },
    /// Cancel one exact order.
    Cancel {
        /// Zero-based contiguous batch position.
        batch_index: u16,
        /// Exact instrument.
        instrument_id: InstrumentId,
        /// Exact original client order ID.
        client_order_id: ClientOrderId,
        /// Unsupported venue order ID, required to remain absent.
        venue_order_id: Option<VenueOrderId>,
        /// Unsupported replacement cancel ID, required to remain absent.
        cancel_request_client_order_id: Option<ClientOrderId>,
    },
}

impl ExecutionWriteElementViewV1 {
    /// Returns the exact client order ID represented by this element.
    #[must_use]
    pub const fn client_order_id(&self) -> ClientOrderId {
        match self {
            Self::Submit {
                client_order_id, ..
            }
            | Self::Modify {
                client_order_id, ..
            }
            | Self::Cancel {
                client_order_id, ..
            } => *client_order_id,
        }
    }

    /// Returns the exact instrument represented by this element.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Submit { instrument_id, .. }
            | Self::Modify { instrument_id, .. }
            | Self::Cancel { instrument_id, .. } => *instrument_id,
        }
    }
}

/// Complete non-authoritative native view of one final unsigned request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionWritePayloadViewV1 {
    /// Schema version, exactly one.
    pub schema_version: u8,
    /// Exact mutation kind.
    pub kind: ExecutionWriteKind,
    /// Exact execution client.
    pub client_id: ClientId,
    /// Exact venue account.
    pub account_id: AccountId,
    /// Exact product.
    pub product: ExecutionProduct,
    /// Exact route.
    pub route: ExecutionRoute,
    /// Exact transport target.
    pub transport: ExecutionTransportTargetViewV1,
    /// Ordered native request elements.
    pub elements: Vec<ExecutionWriteElementViewV1>,
    /// Exact retry ancestry, present only for reauthorized retry-as-submit.
    pub retry_of: Option<FinalWritePayloadDigest>,
}

impl ExecutionWritePayloadViewV1 {
    /// Validates the fixed first-release request policy and exact lean-context parity.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed context, unsupported transport or order control, or any
    /// mismatch between the command identity and final native request.
    pub fn validate_first_release(
        &self,
        context: &ExecutionWriteContext,
    ) -> Result<(), ExecutionWriteGateError> {
        if self.schema_version != 1 || self.elements.len() != 1 {
            return Err(ExecutionWriteGateError::InvalidContext(
                "first release requires schema one and one request element".to_string(),
            ));
        }
        if context.command_id == UUID4::default()
            || context.client_id != self.client_id
            || context.account_id != self.account_id
            || context.route != self.route
            || context.kind != self.kind
        {
            return Err(ExecutionWriteGateError::InvalidContext(
                "command, client, account, route, or mutation kind mismatch".to_string(),
            ));
        }
        let element = &self.elements[0];
        if context.client_order_ids != [element.client_order_id()]
            || context.instrument_id != Some(element.instrument_id())
        {
            return Err(ExecutionWriteGateError::InvalidContext(
                "instrument or exact client order identity mismatch".to_string(),
            ));
        }
        if !matches!(
            (self.route, self.product),
            (ExecutionRoute::BinanceSpot, ExecutionProduct::Spot)
                | (ExecutionRoute::BinanceFutures, ExecutionProduct::Futures)
        ) {
            return Err(ExecutionWriteGateError::InvalidContext(
                "product and route mismatch".to_string(),
            ));
        }
        self.validate_transport()?;
        self.validate_retry_shape()?;
        match element {
            ExecutionWriteElementViewV1::Submit { .. } => self.validate_submit(),
            ExecutionWriteElementViewV1::Cancel { .. } => self.validate_cancel(),
            ExecutionWriteElementViewV1::Modify { .. } => Err(
                ExecutionWriteGateError::Unsupported(ExecutionWriteKind::Modify),
            ),
        }
    }

    fn validate_transport(&self) -> Result<(), ExecutionWriteGateError> {
        let (method_matches, route_matches, recv_window_ms) = match &self.transport {
            ExecutionTransportTargetViewV1::Http {
                method,
                path,
                recv_window_ms,
            } => {
                let method_matches = matches!(
                    (self.kind, *method),
                    (
                        ExecutionWriteKind::Submit | ExecutionWriteKind::Retry,
                        ExecutionHttpMethod::Post
                    ) | (ExecutionWriteKind::Cancel, ExecutionHttpMethod::Delete)
                );
                let route_matches = matches!(
                    (self.route, *path),
                    (
                        ExecutionRoute::BinanceSpot,
                        ExecutionEndpointPath::SpotOrder
                    ) | (
                        ExecutionRoute::BinanceFutures,
                        ExecutionEndpointPath::FuturesOrder
                    )
                );
                (method_matches, route_matches, recv_window_ms)
            }
            ExecutionTransportTargetViewV1::WebSocketApi {
                method,
                recv_window_ms,
            } => {
                let method_matches = matches!(
                    (self.kind, method.as_str()),
                    (
                        ExecutionWriteKind::Submit | ExecutionWriteKind::Retry,
                        "order.place"
                    ) | (ExecutionWriteKind::Cancel, "order.cancel")
                );
                (method_matches, true, recv_window_ms)
            }
        };
        if !method_matches || !route_matches || !(1..=60_000).contains(recv_window_ms) {
            return Err(ExecutionWriteGateError::InvalidContext(
                "transport target is outside the first-release allowlist".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_retry_shape(&self) -> Result<(), ExecutionWriteGateError> {
        let valid = match self.kind {
            ExecutionWriteKind::Retry => self.retry_of.is_some(),
            _ => self.retry_of.is_none(),
        };
        if !valid {
            return Err(ExecutionWriteGateError::InvalidContext(
                "retry ancestry does not match mutation kind".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_submit(&self) -> Result<(), ExecutionWriteGateError> {
        if !matches!(
            self.kind,
            ExecutionWriteKind::Submit | ExecutionWriteKind::Retry
        ) {
            return Err(ExecutionWriteGateError::InvalidContext(
                "submit element does not match mutation kind".to_string(),
            ));
        }
        let ExecutionWriteElementViewV1::Submit {
            batch_index,
            order_type,
            time_in_force,
            quantity,
            price,
            trigger_price,
            quote_quantity,
            display_quantity,
            reduce_only,
            post_only,
            position_side,
            close_position,
            working_type,
            price_protect,
            response_mode,
            good_till_date_ms,
            self_trade_prevention_mode,
            trailing_delta,
            activation_price,
            callback_rate,
            price_match,
            strategy_id,
            strategy_type,
            ..
        } = &self.elements[0]
        else {
            unreachable!("caller selected submit element");
        };
        let order_shape_valid = match order_type {
            OrderType::Market => {
                price.is_none()
                    && *time_in_force == ExecutionTimeInForce::NotApplicable
                    && !*post_only
            }
            OrderType::Limit => {
                price.is_some()
                    && *time_in_force != ExecutionTimeInForce::NotApplicable
                    && (!*post_only || *time_in_force == ExecutionTimeInForce::Gtx)
            }
            _ => false,
        };
        let unsupported_absent = trigger_price.is_none()
            && quote_quantity.is_none()
            && display_quantity.is_none()
            && position_side.is_none()
            && close_position.is_none()
            && working_type.is_none()
            && price_protect.is_none()
            && good_till_date_ms.is_none()
            && self_trade_prevention_mode.is_none()
            && trailing_delta.is_none()
            && activation_price.is_none()
            && callback_rate.is_none()
            && price_match.is_none()
            && strategy_id.is_none()
            && strategy_type.is_none();
        let route_controls_valid = match self.route {
            ExecutionRoute::BinanceSpot => {
                !*reduce_only && *response_mode == ExecutionOrderResponseMode::SpotFull
            }
            ExecutionRoute::BinanceFutures => {
                *response_mode == ExecutionOrderResponseMode::FuturesOmitted
            }
        };
        if *batch_index != 0
            || quantity.raw == 0
            || !order_shape_valid
            || !unsupported_absent
            || !route_controls_valid
        {
            return Err(ExecutionWriteGateError::InvalidContext(
                "submit request is outside the first-release policy".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_cancel(&self) -> Result<(), ExecutionWriteGateError> {
        if self.kind != ExecutionWriteKind::Cancel {
            return Err(ExecutionWriteGateError::InvalidContext(
                "cancel element does not match mutation kind".to_string(),
            ));
        }
        let ExecutionWriteElementViewV1::Cancel {
            batch_index,
            venue_order_id,
            cancel_request_client_order_id,
            ..
        } = &self.elements[0]
        else {
            unreachable!("caller selected cancel element");
        };
        if *batch_index != 0 || venue_order_id.is_some() || cancel_request_client_order_id.is_some()
        {
            return Err(ExecutionWriteGateError::InvalidContext(
                "cancel must use only the exact original client order ID".to_string(),
            ));
        }
        Ok(())
    }
}

/// Lean context supplied with the complete final request to the application gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionWriteContext {
    /// Stable command ID created before persistence activation.
    pub command_id: UUID4,
    /// Exact execution client.
    pub client_id: ClientId,
    /// Exact venue account.
    pub account_id: AccountId,
    /// Exact route.
    pub route: ExecutionRoute,
    /// Exact instrument when singular.
    pub instrument_id: Option<InstrumentId>,
    /// Sorted, unique exact client order IDs.
    pub client_order_ids: Vec<ClientOrderId>,
    /// Exact mutation kind.
    pub kind: ExecutionWriteKind,
}

/// Canonical identity returned by the application gate for the acquired permit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionWriteIdentity {
    /// Digest of the lean context.
    pub context_digest: WriteContextDigest,
    /// Stable command ID.
    pub command_id: UUID4,
    /// Exact execution client.
    pub client_id: ClientId,
    /// Exact venue account.
    pub account_id: AccountId,
    /// Exact route.
    pub route: ExecutionRoute,
    /// Exact instrument when singular.
    pub instrument_id: Option<InstrumentId>,
    /// Sorted, unique exact client order IDs.
    pub client_order_ids: Vec<ClientOrderId>,
    /// Exact mutation kind.
    pub kind: ExecutionWriteKind,
    /// Digest of the complete canonical final request.
    pub final_payload_digest: FinalWritePayloadDigest,
}

/// Digest-only proof of the authority observed by a permit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionAuthorityProof {
    /// Exact authority-state digest.
    pub authority_digest: [u8; 32],
    /// Exact writer-instance digest.
    pub instance_digest: [u8; 32],
    /// Exact writer generation.
    pub generation: u64,
}

/// Final classification recorded before an owned permit is released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportOutcomeClass {
    /// Venue accepted the request into a nonterminal lifecycle.
    AcceptedNonterminal,
    /// Venue or local serializer definitively rejected before an order could exist.
    RejectedDefinitive,
    /// Request bytes may have left without a definitive outcome.
    OutcomeUnknown,
}

/// Typed final-boundary failure.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExecutionWriteGateError {
    /// No gate was injected into a venue-capable factory.
    #[error("execution write gate is missing")]
    MissingGate,
    /// The request kind or endpoint is deny-only in the first release.
    #[error("execution write is unsupported in the first release: {0:?}")]
    Unsupported(ExecutionWriteKind),
    /// The context or native payload is malformed or inconsistent.
    #[error("invalid execution write context: {0}")]
    InvalidContext(String),
    /// The application authority denied the write.
    #[error("execution write authority denied: {0}")]
    Denied(String),
    /// The final request no longer matches the acquired authority.
    #[error("final execution request rejected at arm: {0}")]
    ArmRejected(String),
}

/// Owned final-task permit retained through the transport outcome.
pub trait ExecutionWritePermit: Debug + Send {
    /// Returns the exact canonical identity authorized by this permit.
    fn identity(&self) -> &ExecutionWriteIdentity;

    /// Returns the digest-only authority proof held by this permit.
    fn authority(&self) -> &ExecutionAuthorityProof;

    /// Rechecks the exact native request immediately before authentication and send.
    ///
    /// # Errors
    ///
    /// Returns an error when any final request field differs from the acquired identity.
    fn arm_before_transport(
        &mut self,
        final_payload: &ExecutionWritePayloadViewV1,
    ) -> Result<(), ExecutionWriteGateError>;

    /// Records the transport outcome before releasing the owned permit.
    fn record_outcome(&mut self, outcome: TransportOutcomeClass);
}

/// Application-owned final execution-write gate.
#[async_trait]
pub trait ExecutionWriteGate: Debug + Send + Sync {
    /// Acquires one owned permit for the exact command and complete final native request.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent registration, identity mismatch, stale authority, deny-only
    /// request, or any other fail-closed application decision.
    async fn acquire(
        &self,
        context: &ExecutionWriteContext,
        final_payload: &ExecutionWritePayloadViewV1,
    ) -> Result<Box<dyn ExecutionWritePermit>, ExecutionWriteGateError>;
}

/// Shared injected gate handle.
pub type ExecutionWriteGateHandle = Arc<dyn ExecutionWriteGate>;

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::ClientOrderId;
    use rstest::rstest;

    use super::*;

    fn submit_fixture(
        product: ExecutionProduct,
    ) -> (ExecutionWriteContext, ExecutionWritePayloadViewV1) {
        let route = match product {
            ExecutionProduct::Spot => ExecutionRoute::BinanceSpot,
            ExecutionProduct::Futures => ExecutionRoute::BinanceFutures,
        };
        let path = match product {
            ExecutionProduct::Spot => ExecutionEndpointPath::SpotOrder,
            ExecutionProduct::Futures => ExecutionEndpointPath::FuturesOrder,
        };
        let client_id = ClientId::from(match product {
            ExecutionProduct::Spot => "BINANCE-SPOT",
            ExecutionProduct::Futures => "BINANCE-FUTURES",
        });
        let account_id = AccountId::from(match product {
            ExecutionProduct::Spot => "BINANCE-SPOT-001",
            ExecutionProduct::Futures => "BINANCE-FUTURES-001",
        });
        let instrument_id = InstrumentId::from(match product {
            ExecutionProduct::Spot => "BTCUSDT.BINANCE",
            ExecutionProduct::Futures => "BTCUSDT-PERP.BINANCE",
        });
        let client_order_id = ClientOrderId::from(match product {
            ExecutionProduct::Spot => "SAE-SPOT-1",
            ExecutionProduct::Futures => "SAE-FUTURES-1",
        });
        let context = ExecutionWriteContext {
            command_id: UUID4::new(),
            client_id,
            account_id,
            route,
            instrument_id: Some(instrument_id),
            client_order_ids: vec![client_order_id],
            kind: ExecutionWriteKind::Submit,
        };
        let payload = ExecutionWritePayloadViewV1 {
            schema_version: 1,
            kind: ExecutionWriteKind::Submit,
            client_id,
            account_id,
            product,
            route,
            transport: ExecutionTransportTargetViewV1::Http {
                method: ExecutionHttpMethod::Post,
                path,
                recv_window_ms: 5_000,
            },
            elements: vec![ExecutionWriteElementViewV1::Submit {
                batch_index: 0,
                instrument_id,
                client_order_id,
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: ExecutionTimeInForce::Gtc,
                quantity: Quantity::from("0.001"),
                price: Some(Price::from("50000")),
                trigger_price: None,
                quote_quantity: None,
                display_quantity: None,
                reduce_only: false,
                post_only: false,
                position_side: None,
                close_position: None,
                working_type: None,
                price_protect: None,
                response_mode: match product {
                    ExecutionProduct::Spot => ExecutionOrderResponseMode::SpotFull,
                    ExecutionProduct::Futures => ExecutionOrderResponseMode::FuturesOmitted,
                },
                good_till_date_ms: None,
                self_trade_prevention_mode: None,
                trailing_delta: None,
                activation_price: None,
                callback_rate: None,
                price_match: None,
                strategy_id: None,
                strategy_type: None,
            }],
            retry_of: None,
        };
        (context, payload)
    }

    #[rstest]
    #[case(ExecutionProduct::Spot)]
    #[case(ExecutionProduct::Futures)]
    fn supported_submit_and_exact_cancel_route_parity(#[case] product: ExecutionProduct) {
        let (mut context, mut payload) = submit_fixture(product);
        assert_eq!(payload.validate_first_release(&context), Ok(()));

        context.kind = ExecutionWriteKind::Cancel;
        payload.kind = ExecutionWriteKind::Cancel;
        payload.transport = ExecutionTransportTargetViewV1::Http {
            method: ExecutionHttpMethod::Delete,
            path: match product {
                ExecutionProduct::Spot => ExecutionEndpointPath::SpotOrder,
                ExecutionProduct::Futures => ExecutionEndpointPath::FuturesOrder,
            },
            recv_window_ms: 5_000,
        };
        payload.elements = vec![ExecutionWriteElementViewV1::Cancel {
            batch_index: 0,
            instrument_id: context.instrument_id.unwrap(),
            client_order_id: context.client_order_ids[0],
            venue_order_id: None,
            cancel_request_client_order_id: None,
        }];
        assert_eq!(payload.validate_first_release(&context), Ok(()));
    }

    #[rstest]
    #[case(ExecutionWriteKind::Modify)]
    #[case(ExecutionWriteKind::SubmitList)]
    #[case(ExecutionWriteKind::BatchModify)]
    #[case(ExecutionWriteKind::BatchCancel)]
    #[case(ExecutionWriteKind::CancelAll)]
    fn unsupported_write_classes_are_deny_only(#[case] kind: ExecutionWriteKind) {
        let (mut context, mut payload) = submit_fixture(ExecutionProduct::Spot);
        context.kind = kind;
        payload.kind = kind;
        assert!(payload.validate_first_release(&context).is_err());
    }

    #[test]
    fn websocket_order_transport_requires_exact_method_kind_parity() {
        let (context, mut payload) = submit_fixture(ExecutionProduct::Spot);
        payload.transport = ExecutionTransportTargetViewV1::WebSocketApi {
            method: "order.place".to_string(),
            recv_window_ms: 5_000,
        };
        assert_eq!(payload.validate_first_release(&context), Ok(()));
        payload.transport = ExecutionTransportTargetViewV1::WebSocketApi {
            method: "order.cancel".to_string(),
            recv_window_ms: 5_000,
        };
        assert!(payload.validate_first_release(&context).is_err());
    }

    #[test]
    fn exact_cancel_rejects_venue_and_replacement_id_injection() {
        let (mut context, mut payload) = submit_fixture(ExecutionProduct::Spot);
        context.kind = ExecutionWriteKind::Cancel;
        payload.kind = ExecutionWriteKind::Cancel;
        payload.transport = ExecutionTransportTargetViewV1::Http {
            method: ExecutionHttpMethod::Delete,
            path: ExecutionEndpointPath::SpotOrder,
            recv_window_ms: 5_000,
        };
        payload.elements = vec![ExecutionWriteElementViewV1::Cancel {
            batch_index: 0,
            instrument_id: context.instrument_id.unwrap(),
            client_order_id: context.client_order_ids[0],
            venue_order_id: Some(VenueOrderId::from("123")),
            cancel_request_client_order_id: None,
        }];
        assert!(payload.validate_first_release(&context).is_err());
        let ExecutionWriteElementViewV1::Cancel {
            venue_order_id,
            cancel_request_client_order_id,
            ..
        } = &mut payload.elements[0]
        else {
            unreachable!();
        };
        *venue_order_id = None;
        *cancel_request_client_order_id = Some(ClientOrderId::from("CANCEL-REPLACEMENT"));
        assert!(payload.validate_first_release(&context).is_err());
    }

    #[test]
    fn submit_rejects_every_unsupported_native_control() {
        type Mutation = Box<dyn Fn(&mut ExecutionWriteElementViewV1)>;

        let (context, payload) = submit_fixture(ExecutionProduct::Futures);
        let mut mutations: Vec<Mutation> = vec![
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { trigger_price, .. } = element {
                    *trigger_price = Some(Price::from("49000"));
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { quote_quantity, .. } = element {
                    *quote_quantity = Some(Money::from("50 USDT"));
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit {
                    display_quantity, ..
                } = element
                {
                    *display_quantity = Some(Quantity::from("0.0001"));
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { position_side, .. } = element {
                    *position_side = Some(ExecutionPositionSide::Long);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { close_position, .. } = element {
                    *close_position = Some(true);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { working_type, .. } = element {
                    *working_type = Some(ExecutionWorkingType::MarkPrice);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { price_protect, .. } = element {
                    *price_protect = Some(true);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit {
                    good_till_date_ms, ..
                } = element
                {
                    *good_till_date_ms = Some(1);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit {
                    self_trade_prevention_mode,
                    ..
                } = element
                {
                    *self_trade_prevention_mode = Some("EXPIRE_MAKER".to_string());
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { trailing_delta, .. } = element {
                    *trailing_delta = Some(1);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit {
                    activation_price, ..
                } = element
                {
                    *activation_price = Some(Price::from("49000"));
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { callback_rate, .. } = element {
                    *callback_rate = Some("1.0".to_string());
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { price_match, .. } = element {
                    *price_match = Some("OPPONENT".to_string());
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { strategy_id, .. } = element {
                    *strategy_id = Some(1);
                }
            }),
            Box::new(|element| {
                if let ExecutionWriteElementViewV1::Submit { strategy_type, .. } = element {
                    *strategy_type = Some(1_000_000);
                }
            }),
        ];
        for mutate in mutations.drain(..) {
            let mut candidate = payload.clone();
            mutate(&mut candidate.elements[0]);
            assert!(candidate.validate_first_release(&context).is_err());
        }
    }

    #[test]
    fn retry_requires_explicit_ancestry_and_preserves_context() {
        let (mut context, mut payload) = submit_fixture(ExecutionProduct::Spot);
        context.kind = ExecutionWriteKind::Retry;
        payload.kind = ExecutionWriteKind::Retry;
        assert!(payload.validate_first_release(&context).is_err());
        payload.retry_of = Some(FinalWritePayloadDigest([7; 32]));
        assert_eq!(payload.validate_first_release(&context), Ok(()));
    }
}
