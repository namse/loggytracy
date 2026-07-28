//! Counts object-store operations by kind.
//!
//! This design has been shaped end to end by one number: Cloudflare R2 charges
//! per operation, and per-tenant objects were rejected on that basis alone. The
//! rejection was arithmetic on an estimate, and nothing since has checked the
//! estimate against what the engine actually issues. Amounts cannot be measured
//! locally — a local backend has no price — but *counts* are a property of this
//! code rather than of the backend, so they measure the same on `file://` as on
//! R2.
//!
//! Operation kinds are reported raw rather than pre-multiplied into a bill.
//! Which kinds are billed how is the backend's policy and it changes; what this
//! engine owns is how many of each it issues per flush, merge and retention
//! cycle.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[derive(Debug, Default)]
pub struct ObjectStoreOps {
    pub puts: AtomicU64,
    pub multipart_puts: AtomicU64,
    pub gets: AtomicU64,
    pub deletes: AtomicU64,
    pub lists: AtomicU64,
    /// Objects the list streams yielded. A backend pages a listing, so the
    /// request count is a function of this and the page size, not of `lists`.
    pub listed_objects: AtomicU64,
    pub copies: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectStoreOpCounts {
    pub puts: u64,
    pub multipart_puts: u64,
    pub gets: u64,
    pub deletes: u64,
    pub lists: u64,
    pub listed_objects: u64,
    pub copies: u64,
}

impl ObjectStoreOps {
    pub fn snapshot(&self) -> ObjectStoreOpCounts {
        ObjectStoreOpCounts {
            puts: self.puts.load(Ordering::Relaxed),
            multipart_puts: self.multipart_puts.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
            listed_objects: self.listed_objects.load(Ordering::Relaxed),
            copies: self.copies.load(Ordering::Relaxed),
        }
    }
}

/// Counts every operation reaching `inner`, including the ones issued by
/// `ObjectStore`'s provided methods: `head`, `get_range` and `get` all route
/// through `get_opts`, and `put` through `put_opts`.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    ops: Arc<ObjectStoreOps>,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>, ops: Arc<ObjectStoreOps>) -> Self {
        Self { inner, ops }
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Counted before the call, and regardless of the outcome: a rejected
        // conditional put is a request the backend served and bills for.
        self.ops.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.ops.multipart_puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.ops.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.ops.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.ops.lists.fetch_add(1, Ordering::Relaxed);
        let ops = self.ops.clone();
        self.inner
            .list(prefix)
            .inspect(move |item| {
                if item.is_ok() {
                    ops.listed_objects.fetch_add(1, Ordering::Relaxed);
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.ops.lists.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.list_with_delimiter(prefix).await;
        if let Ok(listed) = &result {
            self.ops
                .listed_objects
                .fetch_add(listed.objects.len() as u64, Ordering::Relaxed);
        }
        result
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.ops.copies.fetch_add(1, Ordering::Relaxed);
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.ops.copies.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_if_not_exists(from, to).await
    }
}
