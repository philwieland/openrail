extern int stomp_connect(const char * const dn, const int port);
extern int stomp_tx(const char * const headers);
extern int stomp_rx(char * headers, const size_t header_size, char * body, const size_t body_size);
extern int stomp_disconnect(void);
