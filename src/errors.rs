//! Common error types used in this crate.

// impl Error for ArrowError {}

// pub type Result<T> = std::result::Result<T, ArrowError>;

error_chain!{
    foreign_links {
        Arrow(::arrow::error::ArrowError);
        DataFusion(::datafusion::error::DataFusionError);
        Fmt(::std::fmt::Error);
        Io(::std::io::Error) #[cfg(unix)];
    }

    // errors {
    //     CommunicationError(t: String) {
    //         description("unable to communicate with k8s")
    //         display("unable to communicate with k8s because: '{}'", t)
    //     }
    // }

    errors {
        Panic(inner: Box<dyn ::std::any::Any + Send + 'static>) {
            description("Thread Panicked")
                display("{}",
                        if let Some(s) = inner.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = inner.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            String::from("Thread Panicked")
                        })
        }
    }
}
