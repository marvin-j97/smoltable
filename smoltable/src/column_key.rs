use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Define the allowed characters
const ALLOWED_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.#$";

pub fn is_valid_identifier(s: &str) -> bool {
    // Check if all characters in the string are allowed
    let all_allowed = s.chars().all(|c| ALLOWED_CHARS.contains(c));

    !s.is_empty() && s.len() < 512 && all_allowed
}

/// The column key allows accessing a column
///
/// It is defined as "family:qualifier", where qualifier may be empty
/// (the colon may omitted in that case).
///
/// A column family may house arbitrarily many columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnKey {
    pub family: String,
    pub qualifier: Option<String>,
}

impl ColumnKey {
    pub fn build_key(&self, row_key: &str) -> String {
        match &self.qualifier {
            Some(cq) => format!("{row_key}:{}:{}:", self.family, cq),
            None => format!("{row_key}:{}:", self.family),
        }
    }
}

impl std::fmt::Display for ColumnKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.family,
            self.qualifier.as_deref().unwrap_or("")
        )
    }
}

impl TryFrom<&str> for ColumnKey {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, ()> {
        let mut splits = value.split(':');

        let family = splits.next();
        let qualifier = splits.next();

        match (family, qualifier) {
            (Some(family), Some("")) | (Some(family), None) => Ok(Self {
                family: family.into(),
                qualifier: None,
            }),
            (Some(family), Some(qualifier)) => {
                if !is_valid_identifier(family) {
                    return Err(());
                }

                Ok(Self {
                    family: family.into(),
                    qualifier: Some(qualifier.to_owned()),
                })
            }
            _ => Err(()),
        }
    }
}

impl Serialize for ColumnKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Call to_string to serialize the struct as a string
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ColumnKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Define a visitor for the ColumnKey struct
        struct ColumnKeyVisitor;

        impl<'de> serde::de::Visitor<'de> for ColumnKeyVisitor {
            type Value = ColumnKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing ColumnKey")
            }

            // Deserialize the struct from a string
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ColumnKey::try_from(value).map_err(|_| {
                    serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)
                })
            }
        }

        deserializer.deserialize_str(ColumnKeyVisitor)
    }
}
