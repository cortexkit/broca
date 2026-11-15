//! Comparisons of the store actor against today's actor that also pin what the store side
//! did (see `broca_test_support::pins`), so each check outlives today's storage.

/// Asserts the store side's value equals today's, as `assert_eq!` does, and pins the store
/// side's value under the assertion's message, or under "alike" when it has none.
macro_rules! alike {
    ($store:expr, $today:expr $(,)?) => {
        match (&$store, &$today) {
            (store, today) => {
                broca_test_support::pin!("alike", store);
                assert_eq!(store, today);
            }
        }
    };
    ($store:expr, $today:expr, $($message:tt)+) => {
        match (&$store, &$today) {
            (store, today) => {
                broca_test_support::pin!(format!($($message)+), store);
                assert_eq!(store, today, $($message)+);
            }
        }
    };
}

/// [`alike!`] with today's value first.
macro_rules! alike_today_first {
    ($today:expr, $store:expr $(,)?) => {
        alike!($store, $today)
    };
    ($today:expr, $store:expr, $($message:tt)+) => {
        alike!($store, $today, $($message)+)
    };
}
