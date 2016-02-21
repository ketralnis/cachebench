// a memcached client.

// This shouldn't really be used as a real client. It's not designed to have a
// stable API or very good error handling, it doesn't validate anything, it has
// no tests, and it's missing significant features like reconnection, connection
// pooling, and multi-server support. It's used for benchmarking our own server

use std::io;
use std::io::{Read,Write};
use std::error::Error;
use std::net::TcpStream;
use std::str::from_utf8;
use std::str::FromStr;

use nom::{crlf, space, digit, IResult};

#[derive(Debug,Eq,PartialEq)]
pub enum Response {
    Ok,
    Stored,
    NotStored,
    NotFound,
    Exists,
    Error,
    ClientError{message: Vec<u8>},
    ServerError{message: Vec<u8>},
    Gets{responses: Vec<SingleGetResponse>},
}

#[derive(Debug,Eq,PartialEq)]
pub struct SingleGetResponse {
    key: Vec<u8>,
    data: Vec<u8>,
    flags: u32,
    unique: u64,
}

pub type ParseResult = Result<Response, ClientError>;

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Simple(&'static str),
    Parse(String),
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> ClientError {
        ClientError::Io(err)
    }
}

fn setter(socket: &mut TcpStream, setter_name: &[u8], key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    try!(socket.write(setter_name));
    try!(socket.write(b" "));
    try!(socket.write(key));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", flags).as_bytes()));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", exptime).as_bytes()));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", data.len()).as_bytes()));
    try!(socket.write(b"\r\n"));
    try!(socket.write(data));
    try!(socket.write(b"\r\n"));
    parse(socket)
}

pub fn set(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    setter(socket, b"set", key, data, flags, exptime)
}

pub fn add(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    setter(socket, b"add", key, data, flags, exptime)
}

pub fn replace(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    setter(socket, b"replace", key, data, flags, exptime)
}

pub fn append(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    setter(socket, b"append", key, data, flags, exptime)
}

pub fn prepend(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32)
    -> ParseResult {
    setter(socket, b"prepend", key, data, flags, exptime)
}

pub fn cas(socket: &mut TcpStream, key: &[u8], data: &[u8], flags: u32, exptime: u32, unique: u64)
    -> ParseResult {
    try!(socket.write(b"cas "));
    try!(socket.write(key));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", flags).as_bytes()));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", exptime).as_bytes()));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", data.len()).as_bytes()));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", unique).as_bytes()));
    try!(socket.write(b"\r\n"));
    try!(socket.write(data));
    try!(socket.write(b"\r\n"));
    parse(socket)
}

fn getter(socket: &mut TcpStream, getter_name: &[u8], keys: &Vec<Vec<u8>>)
    -> ParseResult {
    try!(socket.write(getter_name));
    for key in keys {
        try!(socket.write(b" "));
        try!(socket.write(&key));
    }
    try!(socket.write(b"\r\n"));
    parse(socket)
}

pub fn get(socket: &mut TcpStream, keys: &Vec<Vec<u8>>)
    -> ParseResult {
    getter(socket, b"get", keys)
}

pub fn gets(socket: &mut TcpStream, keys: &Vec<Vec<u8>>)
    -> ParseResult {
    getter(socket, b"gets", keys)
}

pub fn delete(socket: &mut TcpStream, key: &[u8])
    -> ParseResult {
    try!(socket.write(b"delete "));
    try!(socket.write(key));
    try!(socket.write(b"\r\n"));
    parse(socket)
}

fn increr(socket: &mut TcpStream, increr_name: &[u8], key: &[u8], value: u64)
    -> ParseResult {
    try!(socket.write(increr_name));
    try!(socket.write(b" "));
    try!(socket.write(key));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", value).as_bytes()));
    try!(socket.write(b"\r\n"));
    parse(socket)
}

pub fn incr(socket: &mut TcpStream, key: &[u8], value: u64)
    -> ParseResult {
    increr(socket, b"incr", key, value)
}

pub fn decr(socket: &mut TcpStream, key: &[u8], value: u64)
    -> ParseResult {
    increr(socket, b"decr", key, value)
}

pub fn touch(socket: &mut TcpStream, key: &[u8], exptime: u32)
    -> ParseResult {
    try!(socket.write(b"touch "));
    try!(socket.write(key));
    try!(socket.write(b" "));
    try!(socket.write(format!("{}", exptime).as_bytes()));
    try!(socket.write(b"\r\n"));
    parse(socket)
}

pub fn flush_all(socket: &mut TcpStream)
    -> ParseResult {
    try!(socket.write(b"flush_all\r\n"));
    parse(socket)
}

named!(key_parser<&[u8], &[u8]>, is_not!(" \t\r\n\0"));

named!(u32_digit<u32>,
  map_res!(
    map_res!(
      digit,
      from_utf8
    ),
    FromStr::from_str
  )
);

named!(u64_digit<u64>,
  map_res!(
    map_res!(
      digit,
      from_utf8
    ),
    FromStr::from_str
  )
);

named!(usize_digit<usize>,
    map_res!(
        map_res!(
            digit,
            from_utf8
        ),
        FromStr::from_str
    )
);

named!(error<&[u8], Response>,
    chain!(
        tag!("ERROR") ~
        crlf,
        || {
            Response::Error
        }
    )
);

named!(client_error<&[u8], Response>,
    chain!(
        tag!("CLIENT_ERROR") ~
        space ~
        message: is_not!("\r\n\0") ~
        crlf,
        || {
            Response::ClientError{message: message.to_vec()}
        }
    )
);

named!(server_error<&[u8], Response>,
    chain!(
        tag!("SERVER_ERROR") ~
        space ~
        message: is_not!("\r\n\0") ~
        crlf,
        || {
            Response::ServerError{message: message.to_vec()}
        }
    )
);

named!(stored<&[u8], Response>,
    chain!(
        tag!("STORED") ~
        crlf,
        || {
            Response::Stored
        }
    )
);

named!(not_stored<&[u8], Response>,
    chain!(
        tag!("NOT_STORED") ~
        crlf,
        || {
            Response::NotStored
        }
    )
);

named!(exists<&[u8], Response>,
    chain!(
        tag!("EXISTS") ~
        crlf,
        || {
            Response::Exists
        }
    )
);

named!(ok<&[u8], Response>,
    chain!(
        tag!("OK") ~
        crlf,
        || {
            Response::Ok
        }
    )
);

named!(not_found<&[u8], Response>,
    chain!(
        tag!("NOT_FOUND") ~
        crlf,
        || {
            Response::NotFound
        }
    )
);

named!(empty_gets<&[u8], Response>,
    // parses both `get` and `gets` responses
    chain!(
        tag!("END") ~
        crlf,
        || {
            Response::Gets{responses: vec![]}
        }
    )
);


named!(gets_entry<&[u8], SingleGetResponse>,
    // parses both `get` and `gets` responses
    chain!(
        tag!("VALUE") ~
        space ~
        key: key_parser ~
        space ~
        flags: u32_digit ~
        space ~
        bytes: usize_digit ~
        space ~
        unique: u64_digit ~
        crlf ~
        data: take!(bytes) ~
        crlf,
        || {
            SingleGetResponse{key: key.to_vec(), data: data.to_vec(), flags: flags, unique: unique}
        }
    )
);

named!(get_entry<&[u8], SingleGetResponse>,
    // parses both `get` and `gets` responses
    chain!(
        tag!("VALUE") ~
        space ~
        key: key_parser ~
        space ~
        flags: u32_digit ~
        space ~
        bytes: usize_digit ~
        crlf ~
        data: take!(bytes) ~
        crlf,
        || {
            SingleGetResponse{key: key.to_vec(), data: data.to_vec(), flags: flags, unique: 0}
        }
    )
);

named!(get_or_gets_entry<&[u8], SingleGetResponse>,
    alt!(get_entry | gets_entry)
);

named!(get_entries<&[u8], Response>,
    chain!(
        responses: many1!(get_or_gets_entry) ~
        tag!("END") ~
        crlf,
        || {
            Response::Gets{responses: responses}
        }
    )
);

named!(pub parse_response<&[u8], Response>,
    alt!(
        ok | error | client_error | server_error
        | stored | not_stored | exists | not_found
        | empty_gets | get_entries
    )
);

fn parse(socket: &mut TcpStream) -> ParseResult {

    try!(socket.flush());

    let mut buff: [u8; 10240] = [0; 10240];
    let mut parse_state: Vec<u8> = Vec::with_capacity(buff.len());

    loop {
        match try!(socket.read(&mut buff)) {
            0 => {
                return Err(ClientError::Simple("early eof"));
            },
            size => {
                parse_state.extend_from_slice(&buff[0..size]);

                match parse_response(&parse_state) { // TODO copy
                    IResult::Done(remaining, _) if remaining.len()>0 => {
                        return Err(ClientError::Simple("extra data"));
                    },
                    IResult::Done(_, response) => {
                        return Ok(response);
                    },
                    IResult::Incomplete(_needed) => {
                        continue;
                    },
                    IResult::Error(err) => {
                        // TODO NOTE HEY YOU there is a bug in nom
                        // (https://github.com/Geal/nom/issues/226) that makes
                        // it impossible to differentiate between incomplete
                        // data and actual parsing failures. The workaround I
                        // use here is to pretend that that can never happen, so
                        // if we have a parsing error then it must be because
                        // there was incomplete data. This obviously isn't ideal
                        // and will mask bugs in our parser, particularly
                        // commands that the server can send that we just don't
                        // recognise. Hopefully the symptom of this will be the
                        // program hanging or entering an infinite loop
                        // repeatedly failing to parse the same data, which
                        // should be noticeable since we're always run
                        // interactively
                        continue;
                        return Err(ClientError::Parse(
                            format!("some parsing issue: {:?}", err)));
                    },
                }
            }
        }
    }
}

pub fn connect(host: &str, port: u16) -> io::Result<TcpStream> {
    // let addr = SocketAddr::SocketAddrV4(host, port);
    // panics if it can't connect
    TcpStream::connect(&(host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::IResult;
    use nom::Needed;
    use nom::Err;
    use nom::ErrorKind;

    #[test]
    pub fn tests() {
        let tests: Vec<(&str, IResult<&[u8], Response>)> = vec![
            ("ERROR\r\n",
             IResult::Done(b"", Response::Error)),
            ("CLIENT_ERROR you suck\r\n",
             IResult::Done(b"", Response::ClientError{message: b"you suck".to_vec()})),
            ("SERVER_ERROR I suck\r\n",
             IResult::Done(b"", Response::ServerError{message: b"I suck".to_vec()})),
            ("NOT_STORED\r\n",
             IResult::Done(b"", Response::NotStored)),
            ("EXISTS\r\n",
             IResult::Done(b"", Response::Exists)),
            ("OK\r\n",
             IResult::Done(b"", Response::Ok)),
            ("NOT_FOUND\r\n",
             IResult::Done(b"", Response::NotFound)),
            ("END\r\n",
             IResult::Done(b"", Response::Gets{responses: vec![]})),
            ("VALUE thekey 0 4\r\ndata\r\nEND\r\n",
             IResult::Done(b"", Response::Gets { responses: vec![SingleGetResponse { key: b"thekey".to_vec(), data: b"data".to_vec(), flags: 0, unique: 0 }] })),
            ("VALUE thekey1 0 4\r\ndata\r\nVALUE thekey2 0 5\r\ndata!\r\nEND\r\n",
             IResult::Done(b"",  Response::Gets { responses: vec![
                SingleGetResponse { key: b"thekey1".to_vec(), data: b"data".to_vec(), flags: 0, unique: 0 },
                SingleGetResponse { key: b"thekey2".to_vec(), data: b"data!".to_vec(), flags: 0, unique: 0 },
            ]})),
            ("VALUE thekey 0 4 150\r\ndata\r\nEND\r\n",
             IResult::Done(b"", Response::Gets { responses: vec![SingleGetResponse { key: b"thekey".to_vec(), data: b"data".to_vec(), flags: 0, unique: 150 }] })),
            ("VALUE thekey1 0 4 150\r\ndata\r\nVALUE thekey2 0 5 175\r\ndata!\r\nEND\r\n",
             IResult::Done(b"",  Response::Gets { responses: vec![
                SingleGetResponse { key: b"thekey1".to_vec(), data: b"data".to_vec(), flags: 0, unique: 150 },
                SingleGetResponse { key: b"thekey2".to_vec(), data: b"data!".to_vec(), flags: 0, unique: 175 },
            ]})),

            // NOTE if these tests ever start failing, the nom bug documented in
            // parse() has probably been fixed so the tests and that workaround
            // should be changed to check for the proper IResult::Incomplete
            // value
            ("V",
             IResult::Incomplete(Needed::Size(5))),
            ("VALUE",
             IResult::Error(Err::Position(ErrorKind::Alt, b"VALUE"))),
            ("VALUE ",
            IResult::Error(Err::Position(ErrorKind::Alt, b"VALUE "))),
            ("VALUE thekey1",
            IResult::Error(Err::Position(ErrorKind::Alt, b"VALUE thekey1"))),
        ];

        for &(ref response, ref expected_result) in &tests {
            println!("response: {:?}", *response);
            let parsed = parse_response(response.as_bytes());
            println!("expect:  {:?}", expected_result);
            println!("got:     {:?}", parsed);
            if *expected_result == parsed {
                println!("good!");
            } else {
                println!("bad :(");
            }
            assert_eq!(*expected_result, parsed);
        }

    }

}
