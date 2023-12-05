use crate::identifier::is_valid_identifier;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedColumnKey {
    pub family: String,
    pub qualifier: Option<String>,
}

impl std::fmt::Display for ParsedColumnKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.family,
            self.qualifier.as_deref().unwrap_or("")
        )
    }
}

impl TryFrom<&str> for ParsedColumnKey {
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

                if !is_valid_identifier(qualifier) {
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

impl Serialize for ParsedColumnKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Call to_string to serialize the struct as a string
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ParsedColumnKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Define a visitor for the ParsedColumnKey struct
        struct ParsedColumnKeyVisitor;

        impl<'de> serde::de::Visitor<'de> for ParsedColumnKeyVisitor {
            type Value = ParsedColumnKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing ParsedColumnKey")
            }

            // Deserialize the struct from a string
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ParsedColumnKey::try_from(value).map_err(|_| {
                    serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)
                })
            }
        }

        deserializer.deserialize_str(ParsedColumnKeyVisitor)
    }
}

pub type ColumnKey = ParsedColumnKey;
