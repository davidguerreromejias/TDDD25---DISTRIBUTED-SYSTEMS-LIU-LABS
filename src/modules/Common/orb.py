# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

import threading
import socket
import json
import builtins

"""Object Request Broker

This module implements the infrastructure needed to transparently create
objects that communicate via networks. This infrastructure consists of:

--  Strub ::
        Represents the image of a remote object on the local machine.
        Used to connect to remote objects. Also called Proxy.
--  Skeleton ::
        Used to listen to incoming connections and forward them to the
        main object.
--  Peer ::
        Class that implements basic bidirectional (Stub/Skeleton)
        communication. Any object wishing to transparently interact with
        remote objects should extend this class.

"""


class ComunicationError(Exception):
    pass


class Stub(object):

    """ Stub for generic objects distributed over the network.

    This is  wrapper object for a socket.

    """

    def __init__(self, address):
        self.address = tuple(address)

    def _rmi(self, method, *args):
        #
        # Your code here.
        #
        # should parse and send json requests and send these to skeleton of the requested peer
        message = json.dumps({"method": method, "args": args})
        # for serialized json
        message += "\n"
        try:
            transmission_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            transmission_socket.connect(self.address)
            data = transmission_socket.makefile(mode="rw")
            data.write(message)
            data.flush()
            result = json.loads(data.readline())
            data.flush()
            transmission_socket.close()
            #print(result)
            if 'error' in result:
                # Maybe fix a more specific error
                raise Exception("errorrr")
            return result['result']
        except Exception as e:
            print("\t{}: {}".format(type(e),e))
        finally:
            transmission_socket.close()
        pass

    def __getattr__(self, attr):
        """Forward call to name over the network at the given address."""
        def rmi_call(*args):
            return self._rmi(attr, *args)
        return rmi_call


class Request(threading.Thread):

    """Run the incoming requests on the owner object of the skeleton."""

    def __init__(self, owner, conn, addr):
        threading.Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.owner = owner
        self.daemon = True

    def run(self):
        #
        # Your code here.
        #
        try:
            worker = self.conn.makefile(mode="rw")
            request = worker.readline()
            result = self.owner.handle_request(request)
            # newline might not be needed
            worker.write(result + '\n')
            worker.flush()
        except Exception as e:
            print("\t{}: {}".format(type(e), e))
        finally:
            self.conn.close()
        
    
    def handle_request(request):
        try:
            print("Request::handle_request() : Entering function")
            incoming = json.loads(request)
            # not sure if the output is correct from the __getattr__
            type_of_object = __getattr__(self.owner,incoming['method'])(*incoming['args'])
            print(type_of_object)
            result = json.dumps({"result": type_of_object})
        except AttributeError as e:
            print("\t{}: {}".format(type(e), e))
            result = json.dumps({"error" : {"name": type(e), "args": e}})
        finally:
            print("Request::handle_request() : Exiting function")
            return result


class Skeleton(threading.Thread):

    """ Skeleton class for a generic owner.

    This is used to listen to an address of the network, manage incoming
    connections and forward calls to the generic owner class.

    """

    def __init__(self, owner, address):
        threading.Thread.__init__(self)
        self.address = address
        self.owner = owner
        self.daemon = True
        #
        # Your code here.
        #
        # create a socket? and pass? connect before send?
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(address)
        # The argument to listen() is just for protection from multiple requests
        self.server_socket.listen(1)
        
        #self.request = Request(self, s, address)
        
        pass

    def run(self):
        #
        # Your code here.
        #
        while True:
            try:
                conn, addr = self.server_socket.accept()
                new_request = Request(self.owner,conn,addr)
                print("Serving a new request from {0}".format(addr))
                new_request.start()
            except socket.error:
                print("SocketError")
                
        # first call by peer
        # listen for requests
        # when we got a request check if the owner is still alive
        pass


class Peer:

    """Class, extended by objects that communicate over the network."""

    def __init__(self, l_address, ns_address, ptype):
        self.type = ptype
        self.hash = ""
        self.id = -1
        self.address = self._get_external_interface(l_address)
        self.skeleton = Skeleton(self, self.address)
        self.name_service_address = self._get_external_interface(ns_address)
        self.name_service = Stub(self.name_service_address)

    # Private methods

    def _get_external_interface(self, address):
        """ Determine the external interface associated with a host name.

        This function translates the machine's host name into its the
        machine's external address, not into '127.0.0.1'.

        """

        addr_name = address[0]
        if addr_name != "":
            addrs = socket.gethostbyname_ex(addr_name)[2]
            if len(addrs) == 0:
                raise ComunicationError("Invalid address to listen to")
            elif len(addrs) == 1:
                addr_name = addrs[0]
            else:
                al = [a for a in addrs if a != "127.0.0.1"]
                addr_name = al[0]
        addr = list(address)
        addr[0] = addr_name
        return tuple(addr)

    # Public methods

    def start(self):
        """Start the communication interface."""

        self.skeleton.start()
        #print(self.type, self.address)
        self.id, self.hash = self.name_service.register(self.type, self.address)

    def destroy(self):
        """Unregister the object before removal."""

        self.name_service.unregister(self.id, self.type, self.hash)

    def check(self):
        """Checking to see if the object is still alive."""

        return (self.id, self.type)
