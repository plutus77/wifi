#!/usr/bin/env python
#
# Simple TCP line-based text chat server.  Clients connect and send a
# conversation ID (any string) as the first line, then they are connected
# to other clients who sent the same conversation ID.
#
# Uses a thread pair per connection, so not highly scalable.

from Queue import Queue, Empty
from SocketServer import StreamRequestHandler, ThreadingTCPServer
from collections import defaultdict
from threading import Lock, Thread
from socket import error

conversations = defaultdict(set)
"""Key is a conversation ID string, value is a set of ConnectionHandlers
that use that ID"""

conversations_lock = Lock()
"""Lock held while accessing conversations"""


def print_conversation(conversation_id, handlers):
    """Prints a line about which handlers are attached to a conversation"""
    handler_addrs = sorted(['%s:%d' % h.client_address for h in handlers])
    handlers_str = '(%s)' % (', '.join(handler_addrs))
    print('conversation "%s" -> %s' % (conversation_id, handlers_str))


class ConversationConnectionHandler(StreamRequestHandler):
    """Handles TCP connections with the conversation protocol"""

    def __init__(self, request, client_address, server):
        self._conversation_id = None
        self._messages = Queue()
        # This is a blocking call, so we have to set our fields before calling
        StreamRequestHandler.__init__(self, request, client_address, server)

    def setup(self):
        print('connect %s:%d' % self.client_address)
        StreamRequestHandler.setup(self)

    def finish(self):
        StreamRequestHandler.finish(self)
        # Remove this handler from the conversations dict
        print('close %s:%d' % self.client_address)
        if self._conversation_id is not None:
            with conversations_lock:
                handlers = conversations[self._conversation_id]
                handlers.remove(self)
                if not handlers:
                    del conversations[self._conversation_id]
                print_conversation(self._conversation_id, handlers)
            self._conversation_id = None

    def handle(self):
        # The first line is the conversation ID
        line = self.rfile.readline()
        if not line:
            # Client disconnected or declined or sent an empty line
            return

        self._conversation_id = line.strip()

        # Register this handler instance for the conversation ID
        with conversations_lock:
            handlers = conversations[self._conversation_id]
            handlers.add(self)
            print_conversation(self._conversation_id, handlers)

        # Spawn another thread to handle writes
        Thread(target=self._write_handler).start()

        while True:
            try:
                line = self.rfile.readline()
            except error:
                # Client disconnected or other socket error
                break
            if not line:
                # Client disconnected
                break
            # Send the message to each connected client
            with conversations_lock:
                for handler in conversations[self._conversation_id]:
                    if handler is not self:
                        handler._messages.put(line)

    def _write_handler(self):
        while not self.rfile.closed and not self.wfile.closed:
            try:
                # Get the next message we should write from the queue.
                # A short timeout lets us detect a closed socket (otherwise
                # the thread would only discover it on the next attempt
                # to write a message).
                message = self._messages.get(block=True, timeout=1)
                try:
                    self.wfile.write(message)
                    self.wfile.flush()
                except error:
                    # The connection probably dropped; end the handler
                    break
            except Empty:
                # Queue was empty at timeout; just keep going
                pass


class TCPReuseAddrServer(ThreadingTCPServer):
    """Extends ThreadingTCPServer to enable address reuse"""
    allow_reuse_address = True


def main():
    host = '0.0.0.0'
    port = 54321
    listen_addr = (host, port)

    server = TCPReuseAddrServer(listen_addr, ConversationConnectionHandler)
    print('listening on %s:%d' % listen_addr)
    server.serve_forever()


if __name__ == '__main__':
    main()
