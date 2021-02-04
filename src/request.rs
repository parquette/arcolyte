use cookie::CookieJar;
use reqwest::{self, Method, Url};
use reqwest::header::{Cookie, Headers};


/// A HTTP request.
#[derive(Debug, Clone)]
pub struct Request {
    pub destination: Url,
    pub method: Method,
    pub headers: Headers,
    pub cookies: CookieJar,
    pub body: Option<Vec<u8>>,
}

impl Request {
    pub fn new(destination: Url, method: Method) -> Request {
        let headers = Headers::default();
        let cookies = CookieJar::default();
        let body = None;

        Request {
            destination,
            method,
            headers,
            cookies,
            body,
        }
    }

    pub(crate) fn to_reqwest(&self) -> reqwest::Request {
        let mut r = reqwest::Request::new(self.method.clone(), self.destination.clone());

        r.headers_mut().extend(self.headers.iter());

        let mut cookie_header = Cookie::new();

        for cookie in self.cookies.iter() {
            cookie_header.set(cookie.name().to_owned(), cookie.value().to_owned());
        }
        r.headers_mut().set(cookie_header);

        r
    }
}

