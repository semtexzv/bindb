#![feature(associated_type_defaults)]
#![feature(trace_macros)]

pub mod ord;

use std::borrow::Cow;
use crate::ord::Ordered;
use heed::types::{DecodeIgnore, SerdeJson};
use heed::{BytesDecode, BytesEncode, RoTxn, RwTxn};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::path::Path;
pub mod prelude {
    pub use crate::{index, table, Index, Table, Format, ord::Ordered};
    pub use heed::types::{SerdeJson};
}
use protokit::BinProto;

type KeyType<T> = Ordered<<T as Table>::Key>;
type ValType<T> = SerdeJson<T>;

pub trait Format<'a, T>: BytesEncode<'a> + BytesDecode<'a> {}

pub struct Proto<T>(PhantomData<T>);

impl<'a, T: BinProto<'a> + 'a> BytesEncode<'a> for Proto<T> {
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error>> {
        protokit::binformat::encode(item).map(Cow::Owned).map_err(Into::into)
    }
}

impl<'a, T: BinProto<'a> + Default + 'a> BytesDecode<'a> for Proto<T> {
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        protokit::binformat::decode(bytes).map_err(Into::into)
    }
}

impl<'a, T: Serialize + DeserializeOwned + 'a> Format<'a, T> for Ordered<T> {}

impl<'a, T: Serialize + DeserializeOwned + 'a> Format<'a, T> for SerdeJson<T> {}

impl<'a, T: Default + BinProto<'a> + 'a> Format<'a, T> for Proto<T> {}


/// Types which should be stored.
pub trait Table: Serialize + DeserializeOwned {
    /// Name of the table. This should be unique within database
    const NAME: &'static str;
    type Format<'a>: Format<'a, Self> where Self: 'a;

    type Indices: Indices<Self>;

    /// Primary key of the table. If this type is sane, then it should have same ordering
    /// in rust as it does in its bincode serialized form. Simply - Fields sorted in-order
    /// numeric values sorted naturally, and strings lexicographically.
    type Key: PartialOrd + Serialize + DeserializeOwned;
    type KeyRef<'a>: PartialOrd + Serialize;

    fn get(&self) -> &Self::Key;
    fn get_mut(&mut self) -> &mut Self::Key;
}

#[doc(hidden)]
#[macro_export]
macro_rules! __default_name {
    ($a:literal $b:ty) => {$a};
    ($b:ty) => {stringify!($b)};
}

#[macro_export]
macro_rules! table {
    ($table:ty $(as $name:literal)? ; $fmt:ty $($(=> $pkey:tt)+: $keytype:ty),* $(,$idx:ty)*) => {
        #[allow(unused_parens)]
        impl Table for $table {
            const NAME: &'static str = $crate::__default_name!($($name)? $table);

            type Format<'a> = $fmt;
            type Indices = ($($idx,)*);

            type Key = ($($keytype),*);
            type KeyRef<'a> = ( $(&'a $keytype),+ );
            type KeyMut<'a> = ( $(&'a mut $keytype),+ );

            fn get(&self) -> Self::KeyRef {
                ($(&self.$($pkey).* ),*)
            }
            fn get_mut(&mut self) -> &mut Self::Key {
                ($(&mut self.$($pkey).*),*)
            }
        }
    };
}

pub trait Index {
    type Table: Table;
    /// Name of the table backing this index. This should be unique within database.
    const NAME: &'static str;
    type Key: PartialOrd + Serialize + DeserializeOwned;
    type KeyRef<'a>: PartialOrd + Serialize;

    fn get<'a>(t: &'a Self::Table) -> Self::KeyRef<'a>;
}

#[macro_export]
macro_rules! index {
    ($vis:vis $index:ident $(as $name:literal)?, $src:ty, $($(=> $pkey:tt)+: $keytype:ty),*) => {
        $vis struct $index;
        #[allow(unused_parens)]
        impl Index for $index {
            type Table = $src;
            const NAME: &'static str = $crate::__default_name!($($name)? $index);
            type Key = ( $($keytype),+ );
            type KeyRef<'a> = ( $(&'a $keytype),+ );

            fn get<'a>(t : &'a Self::Table) -> Self::KeyRef<'a> {
                ($( &t.$($pkey).+),+)
            }
        }
    };
}

pub trait Indices<T> {
    fn on_register(db: Database) -> Database;
    fn on_update<'a>(db: &Database, tx: &mut RwTxn<'a, 'a>, old: &T, new: &T);
    fn on_insert<'a>(db: &Database, tx: &mut RwTxn<'a, 'a>, t: &T);
    fn on_delete<'a>(db: &Database, tx: &mut RwTxn<'a, 'a>, t: &T);
}

#[impl_trait_for_tuples::impl_for_tuples(6)]
#[tuple_types_no_default_trait_bound]
impl<T> Indices<T> for Tuple
    where
        T: Table,
{
    for_tuples!(where #(Tuple: Index<Table=T>)*);

    fn on_register(mut db: Database) -> Database {
        for_tuples!( #( db = db.register_idx::<Tuple>();)* );
        db
    }

    #[inline(always)]
    fn on_update<'a>(db: &Database, tx: &mut RwTxn<'a, 'a>, old: &T, new: &T) {
        for_tuples!( #({
            let db_inner = db.index_db_ref::<Tuple>();
            let oldkey = Tuple::get(&old);
            let newkey = Tuple::get(&new);
            if oldkey != newkey {
                db_inner.delete(tx, &oldkey).unwrap();
                db_inner.put(tx, &newkey,  Tuple::Table::get(&new)).unwrap();
            }
        })*);
    }

    #[inline(always)]
    fn on_insert<'a>(db: &Database, tx: &mut RwTxn<'a, 'a>, t: &T) {
        for_tuples!( #({
            let db_inner = db.index_db_ref::<Tuple>();
            let key = Tuple::get(&t);
            let prim = Tuple::Table::get(&t);
            if let Some(old) = db_inner.get(tx, &key).unwrap() {
                if &old != prim {
                    panic!("Index detected an attribute change on insert");
                }
            }
            db_inner.put(tx, &key, prim).unwrap();
        })*);
    }

    fn on_delete<'a>(db: &Database, tx: &mut RwTxn<'a, 'a, ()>, t: &T) {
        for_tuples!( #({
            let inner_db = db.index_db_ref::<Tuple>();
            inner_db.delete(tx, &Tuple::get(&t)).unwrap();
        })*);
    }
}

#[derive(Clone)]
pub struct Database {
    tree: heed::Env,
    dbs: HashMap<String, heed::UntypedDatabase>,
}

impl Database {
    pub fn open(f: impl AsRef<Path>) -> Self {
        unsafe {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&f)
                .unwrap();

            let db = heed::EnvOpenOptions::new()
                .max_dbs(256)
                .max_readers(32)
                .map_size(1024 * 1024 * 1024 * 1024)
                .flag(heed::flags::Flags::MdbNoSubDir)
                .open(f)
                .unwrap();

            Database {
                tree: db,
                dbs: HashMap::new(),
            }
        }
    }

    pub fn register<T: Table>(mut self) -> Self {
        let db = self.tree.create_database(Some(T::NAME)).unwrap();
        self.dbs.insert(T::NAME.to_string(), db);
        T::Indices::on_register(self)
    }

    pub fn register_idx<I: Index>(mut self) -> Self {
        let db = self.tree.create_database(Some(I::NAME)).unwrap();
        self.dbs.insert(I::NAME.to_string(), db);
        self
    }

    pub fn clear<T: Table>(&mut self) {
        let d = self.dbs.remove(T::NAME).unwrap();
        let mut w = self.tree.write_txn().unwrap();
        d.clear(&mut w).unwrap();
        w.commit().unwrap();
    }

    pub fn tx(&self) -> Tx<'_> {
        Tx {
            db: self,
            tx: self.tree.read_txn().unwrap(),
        }
    }

    pub fn in_tx<R, F: FnOnce(&Tx) -> R>(&self, f: F) -> R {
        let tx = self.tx();
        let res = f(&tx);
        tx.commit();
        return res;
    }

    pub fn wtx(&self) -> Wtx<'_> {
        Wtx {
            db: self,
            tx: self.tree.write_txn().unwrap(),
        }
    }

    pub fn in_wtx<R, F: FnOnce(&mut Wtx) -> R>(&self, f: F) -> R {
        let mut tx = self.wtx();
        let res = f(&mut tx);
        tx.commit();
        return res;
    }
}

impl Database {
    pub fn untyped_db<T: Table>(&self) -> heed::Database<DecodeIgnore, DecodeIgnore> {
        self.dbs
            .get(T::NAME)
            .expect("Table not registered")
            .remap_types()
    }
    pub fn format_db<'a, T: Table, F: Format<'a, T>>(&self) -> heed::Database<Ordered<T::Key>, F> {
        self.dbs
            .get(T::NAME)
            .expect("Table not registered")
            .remap_types()
    }
    pub fn typed_db<T: Table>(&self) -> heed::Database<Ordered<T::Key>, SerdeJson<T>> {
        self.dbs
            .get(T::NAME)
            .expect("Table not registered")
            .remap_types()
    }
    pub fn index_db<I: Index>(
        &self,
    ) -> heed::Database<Ordered<I::Key>, Ordered<<I::Table as Table>::Key>> {
        self.dbs
            .get(I::NAME)
            .expect("Index not registered")
            .remap_types()
    }
    pub fn index_db_ref<'a, I: Index>(
        &self,
    ) -> heed::Database<Ordered<I::KeyRef<'a>>, Ordered<<I::Table as Table>::Key>> {
        self.dbs
            .get(I::NAME)
            .expect("Index not registered")
            .remap_types()
    }
}

pub struct Iter<'a, T: Table> {
    i: heed::RoRange<'a, KeyType<T>, ValType<T>>,
}

impl<'a, T: Table + 'static> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next().map(|v| v.unwrap().1)
    }
}

pub struct RevIter<'a, T: Table> {
    i: heed::RoRevRange<'a, KeyType<T>, ValType<T>>,
}

impl<'a, T: Table + 'static> Iterator for RevIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next().map(|v| v.unwrap().1)
    }
}


pub trait CommitOps {
    fn commit(self);
}

pub trait ROps {
    fn _ro_tx(&self) -> (&Database, &RoTxn);

    /// Index lookup
    fn get_by<'a, I: Index>(&self, ikey: I::KeyRef<'a>) -> Option<I::Table> {
        let (db, tx) = self._ro_tx();
        let idb = db.index_db_ref::<I>();
        let tdb = db.typed_db::<I::Table>();

        if let Some(pkey) = idb.get(tx, &ikey).unwrap() {
            tdb.get(tx, &pkey).unwrap()
        } else {
            None
        }
    }

    /// Perform a primary key lookup
    fn get<T: Table>(&self, k: &T::Key) -> Option<T> {
        let (db, tx) = self._ro_tx();
        let db = db.typed_db::<T>();

        let res = db.get(&tx, k).unwrap();
        res
    }

    /// Perform a full table scan
    fn scan<T: Table + 'static>(&self) -> Iter<T> {
        let (db, tx) = self._ro_tx();

        let db = db.typed_db::<T>();
        let r = db.range(&tx, &(..)).unwrap();

        Iter { i: r }
    }
    fn rscan<T: Table + 'static>(&self) -> RevIter<T> {
        let (db, tx) = self._ro_tx();

        let db = db.typed_db::<T>();
        let r = db.rev_range(&tx, &(..)).unwrap();

        RevIter { i: r }
    }
}

pub trait RwOps<'a>: ROps {
    fn _rw_tx(&mut self) -> (&Database, &mut RwTxn<'a, 'a>);

    /// Saves the item, just overwriting all indexes
    fn save<T: Table>(&mut self, v: &T) {
        let (dd, mut tx) = self._rw_tx();
        let db = dd.typed_db::<T>();

        // If there is an old version of the row, check for any indexes we might want to upldate
        if let Some(old) = db.get(tx, v.get()).unwrap() {
            db.put(&mut tx, &T::get(&v), &v).unwrap();
            T::Indices::on_update(&dd, &mut tx, &old, &v);
        } else {
            db.put(&mut tx, &T::get(&v), &v).unwrap();
            T::Indices::on_insert(&dd, &mut tx, &v);
        }
    }

    fn delete<T: Table>(&mut self, k: &T::Key) {
        let (db, mut tx) = self._rw_tx();
        let typed = db.typed_db::<T>();

        if let Some(item) = typed.get(&tx, k).unwrap() {
            // If the entry was stored, first update index table and only after that delete the entry
            T::Indices::on_delete(&db, &mut tx, &item);
        }
        typed.delete(&mut tx, k).unwrap();
    }
}

pub struct Tx<'a> {
    db: &'a Database,
    tx: RoTxn<'a>,
}

impl<'a> Tx<'a> {
    pub fn commit(self) {
        self.tx.commit().unwrap();
    }
}

impl<'a> ROps for Tx<'a> {
    fn _ro_tx(&self) -> (&Database, &RoTxn) {
        (&self.db, &self.tx)
    }
}

pub struct Wtx<'a> {
    db: &'a Database,
    tx: RwTxn<'a, 'a>,
}

impl<'a> CommitOps for Wtx<'a> {
    fn commit(self) {
        self.tx.commit().unwrap();
    }
}

impl<'a> ROps for Wtx<'a> {
    fn _ro_tx(&self) -> (&Database, &RoTxn) {
        (&self.db, &self.tx)
    }
}

impl<'a> RwOps<'a> for Wtx<'a> {
    fn _rw_tx(&mut self) -> (&Database, &mut RwTxn<'a, 'a>) {
        (&self.db, &mut self.tx)
    }
}

#[test]
fn test_simple() {
    use crate::{ROps, RwOps};
    use serde::{Deserialize, Serialize};

    #[derive(Default, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
    struct Item(usize, usize);
    table!(Item as "Item"; SerdeJson<Self>
        => 0: usize
    );
    index!(Item0 as "ia", Item, => 0: usize);

    let db = Database::open("/tmp/db").register::<Item>();
    {
        let mut db = db.wtx();
        db.save(&Item(0, 0));
        db.save(&Item(1, 0));
        db.save(&Item(2, 0));
        db.save(&Item(4, 0));
        db.save(&Item(0, 0));
        db.commit();
    }
    let mut db = db.wtx();
    assert_eq!(db.get(&2), Some(Item(2, 0)));

    let range = db.scan::<Item>();
    assert_eq!(range.count(), 4);

    db.delete::<Item>(&0);
    assert_eq!(db.scan::<Item>().count(), 3);
}
