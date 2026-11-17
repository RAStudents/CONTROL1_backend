use crate::{
    domain::{
        access::Identity,
        error::{Error, Result},
    },
    state::App,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, errors::ErrorKind};
pub(crate) fn decode(app: &App, token: &str) -> Result<Identity> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.set_required_spec_claims(&["exp", "sub", "iss", "aud"]);
    validation.set_issuer(&["control"]);
    validation.set_audience(&["control-api"]);
    jsonwebtoken::decode::<Identity>(token, &DecodingKey::from_secret(&app.jwt_key), &validation)
        .map(|v| v.claims)
        .map_err(|e| match e.kind() {
            ErrorKind::ExpiredSignature => Error::new(401, "TOKEN_EXPIRED"),
            _ => Error::new(401, "AUTH_REQUIRED"),
        })
}
