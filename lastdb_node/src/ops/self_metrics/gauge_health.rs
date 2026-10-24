//! Declare gauge-only status records once, including their unit and window.
//!
//! The wire stays a bare u64 or null. Missing fields remain unavailable with
//! their declared metadata, rather than becoming a measured zero.

macro_rules! gauge_health {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident {
            $( $(#[$field_attr:meta])* $field_vis:vis $field:ident:
                ($unit:expr, $window:expr) ),+ $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis struct $name {
            $( $(#[$field_attr])* $field_vis $field: $crate::ops::gauge::Gauge, )+
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $( $field: $crate::ops::gauge::Gauge::field_not_served($unit, $window), )+
                }
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct(
                    stringify!($name), [$(stringify!($field)),+].len(),
                )?;
                $( state.serialize_field(
                    stringify!($field),
                    &$crate::ops::self_metrics::gauge_wire::wire_u64(&self.$field),
                )?; )+
                state.end()
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                struct Raw {
                    $( #[serde(default)] $field: Option<u64>, )+
                }
                let raw = <Raw as serde::Deserialize<'de>>::deserialize(deserializer)?;
                Ok(Self {
                    $( $field: $crate::ops::self_metrics::gauge_wire::gauge_from_wire(
                        raw.$field, $unit, $window,
                    ), )+
                })
            }
        }
    };
}
