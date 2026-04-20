import http.server, os

class Handler(http.server.BaseHTTPRequestHandler):
    def do_PUT(self):
        length = int(self.headers['Content-Length'])
        data = self.rfile.read(length)
        with open('/data/polymarket_v2.db', 'wb') as f:
            f.write(data)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
    def do_GET(self):
        with open('/data/polymarket_v2.db', 'rb') as f:
            data = f.read()
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Disposition', 'attachment; filename="polymarket_v2.db"')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def log_message(self, *a): pass

port = int(os.environ.get('PORT', 8080))
print(f'Upload server on port {port}')
http.server.HTTPServer(('0.0.0.0', port), Handler).serve_forever()