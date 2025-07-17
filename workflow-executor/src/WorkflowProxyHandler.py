import logging
import http.server
import socketserver
import requests
from requests.exceptions import RequestException
import json
import time
import threading
import os
from scheduler import Scheduler

logger  = logging.getLogger(__name__)
handler = logging.Handler()
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(handler)

PROXY_PORT = int(os.environ.get('PROXY_PORT', 8080))
PROXY_ADDRESS = os.environ.get('PROXY_ADDRESS', "0.0.0.0")
TARGET_SERVER = os.environ.get('TARGET_SERVER', "http://0.0.0.0")
TARGET_PORT = int(os.environ.get('TARGET_PORT', 2746))

logger.info(f"--- Starting Scheduler ---")
scheduler = Scheduler(
    TARGET_SERVER,
    TARGET_PORT,
)

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    """
    This handler intercepts client requests, forwards them to the TARGET_SERVER,
    modifies specific responses, and sends them back to the client.
    """ 

    def do_GET(self):
        """Handle GET requests."""
        self.send_response(301)
        self.send_header('Location',f"{TARGET_SERVER}:{TARGET_PORT}")
        self.end_headers()


    def do_POST(self):
        """Handle POST requests."""
        self._forward_and_modify_request("POST")


    def _forward_and_modify_request(self, method):
        """
        The core logic for forwarding requests and modifying responses.
        """
        target_url = f"{TARGET_SERVER}:{TARGET_PORT}{self.path}"
        logger.info(f"Proxying request: {method} {self.path} -> {target_url}")

        request_headers = dict(self.headers)
        request_body = None

        try:
            content_length = int(request_headers.get('Content-Length', 0))
            request_body = self.rfile.read(content_length)
        except (TypeError, ValueError):
            self.send_error(400, "Invalid Content-Length header")
            return
        
        logger.info(f"Request address: {TARGET_SERVER}:{TARGET_PORT}")
        request_headers["Host"] = TARGET_SERVER.split('//')[1].split('/')[0]

        try:

            modified_body = ""
            content_type = request_headers.get("Content-Type", "")
            if method == 'POST' and \
                self.path == '/api/v1/workflows/argo' and \
                'application/json' in content_type:
                
                modified_body = scheduler.schedule_workflow(
                    json.loads(
                        request_body.decode('utf8')
                    )
                )

                modified_body = json.dumps(
                    modified_body
                ).encode()

            real_response = requests.post(
                target_url,
                headers=request_headers,
                data=modified_body,
                timeout=15,
            )
            
            self.send_response(real_response.status_code)
            

            self.end_headers()

            self.wfile.write(real_response.content)
            # self.wfile.close()

        except BrokenPipeError:
            logger.error(f"Broken Pipe Error: Client {self.client_address} disconnected prematurely.")

        except RequestException as e:
            error_message = f"Proxy could not connect to target server: {e}"
            logger.error(error_message)
            self.send_error(502, "Bad Gateway", error_message)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            if not self.headers_sent:
                self.send_error(500, "Internal Server Error", error_message)

def publish_metrics(scheduler):
    logger.info("--- Publishing metrics ---")
    while True:
        time.sleep(5)
        try:
            scheduler.check_publish_metrics()
        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")
            
def run_proxy():
    """
    Starts the proxy server.
    """
    httpd = socketserver.ThreadingTCPServer((PROXY_ADDRESS, PROXY_PORT), ProxyHandler)

    logger.info(f"--- Starting HTTP Proxy Server on port {PROXY_PORT} ---")
    logger.info(f"--- Forwarding requests to: {TARGET_SERVER}:{TARGET_PORT} ---")
    
    try:
        metrics = threading.Thread(target=publish_metrics, args=(scheduler,))
        metrics.start()
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n--- Shutting down the proxy server. ---")
        httpd.shutdown()
        httpd.server_close()
        metrics.join(timeout=2)
        

if __name__ == "__main__":
    run_proxy()