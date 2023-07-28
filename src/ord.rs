use heed::{BytesDecode, BytesEncode};
use ordcode::Order;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;

pub struct Ordered<T>(PhantomData<T>);

impl<'ser, T: Serialize + 'ser> BytesEncode<'ser> for Ordered<T> {
    type EItem = T;

    fn bytes_encode(item: &'ser Self::EItem) -> Result<Cow<'ser, [u8]>, Box<dyn Error>> {
        let key: Cow<'ser, [u8]> = ordcode::ser_to_vec_ordered(item, Order::Ascending)
            .map(Cow::Owned)
            .map_err(Into::<Box<dyn Error>>::into)?;

        if key.len() >= 511 {
            panic!("Key too long");
        }

        Ok(key)
    }
}

impl<'de, T: DeserializeOwned + 'de> BytesDecode<'de> for Ordered<T> {
    type DItem = T;

    fn bytes_decode(bytes: &'de [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        ordcode::de_from_bytes_asc(bytes).map_err(Into::into)
    }
}
