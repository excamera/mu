/* -*-mode:c++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>

#include "socket.hh"
#include "exception.hh"
#include "http_request.hh"
#include "http_response_parser.hh"

using namespace std;

void say_hello();

int main()
{
  try {
    say_hello();
  } catch ( const exception & e ) {
    print_exception( e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

void say_hello()
{
  /* open connection to server */
  TCPSocket www;
  Address server { "www.unitedwifi.com", "http" };
  cerr << "Connecting to " << server.str() << "... ";
  www.connect( server );
  cerr << "done.\n";

  /* prepare request */
  HTTPRequest request;
  request.set_first_line( "GET / HTTP/1.1" );
  request.add_header( HTTPHeader( "Host", server.str() ) );
  request.done_with_headers();
  request.read_in_body( "" );
  assert( request.state() == COMPLETE );

  /* send request */
  cerr << "Sending request... ";
  www.write( request.str() );
  cerr << "done.\n";

  /* construct response parser */
  HTTPResponseParser parser;

  /* tell parser about the request */
  parser.new_request_arrived( request );

  /* read reply */
  while ( not www.eof() ) {
    parser.parse( www.read() );

    /* did we get a reply? */
    if ( not parser.empty() ) {
      cerr << "Got reply.\n";
      const HTTPResponse & response = parser.front();
      for ( unsigned int i = 0; i < response.headers().size(); i++ ) {
        cout << "header # " << i << ": key={" << response.headers().at( i ).key()
             << "}, value={" << response.headers().at( i ).value() << "}\n";
      }

      cout << "Body (" << response.body().size() << " bytes) follows:\n";
      cout << response.body();
      cout << "\n[end of body]\n";

      break; /* we only sent one request */
    }
  }

  www.close();
}
