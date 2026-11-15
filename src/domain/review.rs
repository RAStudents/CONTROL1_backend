//! Decisions for orthogonal acknowledgement and classification state. Persistence
//! serializes event rows; the domain defines which transitions are real changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Acknowledgement {
    New,
    Acknowledged,
    LegacyUnknown,
}
impl Acknowledgement {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "NEW" => Some(Self::New),
            "ACKNOWLEDGED" => Some(Self::Acknowledged),
            "LEGACY_UNKNOWN" => Some(Self::LegacyUnknown),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReviewState {
    pub(crate) acknowledgement: Acknowledgement,
    pub(crate) erroneous: bool,
    pub(crate) classification_version: i64,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReviewAction {
    Acknowledge,
    MarkErroneous {
        expected_classification_version: i64,
    },
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReviewDecision {
    NoOp,
    Acknowledge,
    MarkErroneous,
    VersionConflict,
}
impl ReviewState {
    pub(crate) fn plan(self, action: ReviewAction) -> ReviewDecision {
        match action {
            ReviewAction::Acknowledge => {
                if self.acknowledgement == Acknowledgement::Acknowledged {
                    ReviewDecision::NoOp
                } else {
                    ReviewDecision::Acknowledge
                }
            }
            ReviewAction::MarkErroneous {
                expected_classification_version,
            } => {
                if self.erroneous {
                    ReviewDecision::NoOp
                } else if expected_classification_version != self.classification_version {
                    ReviewDecision::VersionConflict
                } else {
                    ReviewDecision::MarkErroneous
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acknowledgement_and_classification_are_orthogonal() {
        for status in [
            Acknowledgement::New,
            Acknowledgement::LegacyUnknown,
            Acknowledgement::Acknowledged,
        ] {
            let current = ReviewState {
                acknowledgement: status,
                erroneous: false,
                classification_version: 1,
            };
            assert_eq!(
                current.plan(ReviewAction::MarkErroneous {
                    expected_classification_version: 1
                }),
                ReviewDecision::MarkErroneous
            );
        }
    }
    #[test]
    fn erroneous_is_monotonic_and_stale_expected_is_noop() {
        let current = ReviewState {
            acknowledgement: Acknowledgement::New,
            erroneous: true,
            classification_version: 2,
        };
        assert_eq!(
            current.plan(ReviewAction::MarkErroneous {
                expected_classification_version: 1
            }),
            ReviewDecision::NoOp
        );
        assert_eq!(
            current.plan(ReviewAction::Acknowledge),
            ReviewDecision::Acknowledge
        );
    }
    #[test]
    fn conflicts_apply_only_before_first_erroneous_transition() {
        let current = ReviewState {
            acknowledgement: Acknowledgement::Acknowledged,
            erroneous: false,
            classification_version: 1,
        };
        assert_eq!(
            current.plan(ReviewAction::MarkErroneous {
                expected_classification_version: 2
            }),
            ReviewDecision::VersionConflict
        );
        assert_eq!(
            current.plan(ReviewAction::Acknowledge),
            ReviewDecision::NoOp
        );
        assert!(Acknowledgement::parse("ARCHIVED_V2").is_none());
    }
}
