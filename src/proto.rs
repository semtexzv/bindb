use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;
use heed::{BytesDecode, BytesEncode};
#[cfg(feature = "proto")]
use protokit::BinProto;
use crate::Format;

#[cfg(feature = "proto")]
pub struct Proto<T>(PhantomData<T>);

#[cfg(feature = "proto")]
impl<'a, T: BinProto<'a> + 'a> BytesEncode<'a> for Proto<T> {
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error>> {
        protokit::binformat::encode(item)
            .map(Cow::Owned)
            .map_err(Into::into)
    }
}

#[cfg(feature = "proto")]
impl<'a, T: BinProto<'a> + Default + 'a> BytesDecode<'a> for Proto<T> {
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        protokit::binformat::decode(bytes).map_err(Into::into)
    }
}

#[cfg(feature = "proto")]
impl<'a, T: Default + BinProto<'a> + 'a> Format<'a, T> for Proto<T> {}
