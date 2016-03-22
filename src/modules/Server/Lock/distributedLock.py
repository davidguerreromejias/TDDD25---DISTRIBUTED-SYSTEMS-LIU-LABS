# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Rikard-Agravara algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictinaries: request, token,
        and peer_list) should be protected.
    --  the implementation should gratiously handle situations when a
        peer dies unexpectedly. All exceptions comming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated acordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.

"""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN

    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.

        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return dict(token)

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """
        #
        # Your code here
        #

        self.peer_list.lock.acquire()
        # Try catch to make sure we always release the global peer list lock
        try:
            self.request[self.owner.id] = 0
            # Get the peer list with only the IDs
            peers = self.peer_list.get_peers().keys()
            # Instantiate the request list for peers from the peer list
            for peer_id in peers:
                self.request[peer_id] = 0
            # if the list is empty or I have the smallest ID, I get to start with the token
            if len(peers) == 0 or self.owner.id < min(peers):
                self.token = {self.owner.id: self.time}
                self.state = TOKEN_PRESENT
        finally:
            self.peer_list.lock.release()

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        #
        # Your code here.
        #
        if self.state == TOKEN_HELD:
            self.release()

        self.peer_list.lock.acquire()
        try:
            peers = self.peer_list.get_peers().keys()
            print(peers)
            # If there exist other peers and we have the token
            if len(peers) > 0 and self.state == TOKEN_PRESENT:
                sorted_list = sorted(self.request.items())
                got_result = False
                # Should probably put this in a seperate function
                # Loop through the peers and see which peers have issued new requests
                # Ids higher than us i+1,i+2,...,n
                for request_id,request_time in sorted_list:
                    if request_id > self.owner.id and self.token[request_id] < request_time:
                        self.state = NO_TOKEN
                        self.token[self.owner.id] = self.time
                        self.peer_list.peer(request_id).obtain_token(self._prepare(self.token))
                        self.token = None
                        got_result = True
                        break
                # Ids lower than us 1,2,...,i-2,i-1
                if got_result == False:
                    for request_id,request_time in sorted_list:
                        if request_id < self.owner.id and self.token[request_id] < request_time:
                            self.state = NO_TOKEN
                            self.token[self.owner.id] = self.time
                            self.peer_list.peer(request_id).obtain_token(self._prepare(self.token))
                            self.token = None
                            break
                        elif request_id >= self.owner.id:
                            break
        finally:
            self.peer_list.lock.release()

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            # add the calling peer to the request list
            self.request[pid] = 0

            # if we have the token or the token is present, reset the time in token
            if self.state != NO_TOKEN:
                self.token[pid] = 0

        finally:
            self.peer_list.lock.release()

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            # if the peer is registered in token, remove it
            if self.state != NO_TOKEN and pid in self.token.keys():
                self.token.pop(pid,None)
            self.request.pop(pid,None)
        finally:
            self.peer_list.lock.release()

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")
        #
        # Your code here.
        #
        # called when the current peer tries to acquire the lock; if the
        # token is not present, the peer should notify the rest about its desire and
        # suspend its execution until the token is passed to the peer.
        self.peer_list.lock.acquire()
        try:
            self.time += 1
            if self.state == TOKEN_PRESENT:
                self.state = TOKEN_HELD
                return
        finally:
            self.peer_list.lock.release()

        peers = self.peer_list.get_peers().keys()
        for peer_id in peers:
            if peer_id == self.owner.id:
                continue
            self.peer_list.peer(peer_id).request_token(self.time, self.owner.id)

        while self.state == NO_TOKEN:
            pass

        self.peer_list.lock.acquire()
        self.state = TOKEN_HELD
        print("Got token")
        self.peer_list.lock.release()

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        #
        # Your code here.
        #
        #called when the current peer releases the lock; if there are
        #peers waiting for the token, the current peer should pass the token to one
        #of them (carefully think through to whom in order to ensure fairness).
        self.peer_list.lock.acquire()
        try:
            print("1")
            if self.state != NO_TOKEN and len(self.request) > 0:
                self.state = TOKEN_PRESENT
                got_result = False
                print("2")
                sorted_list = sorted(self.request.items())
                # Loop through the peers and see which peers have issued new requests
                # Ids higher than us i+1,i+2,...,n
                for request_id,request_time in sorted_list:
                    if request_id > self.owner.id and self.token[request_id] < request_time:
                        self.state = NO_TOKEN
                        self.token[self.owner.id] = self.time
                        self.peer_list.peer(request_id).obtain_token(self._prepare(self.token))
                        self.token = None
                        got_result = True
                        break

                # Ids lower than us 1,2,...,i-2,i-1
                print("3")
                if got_result == False:
                    for request_id,request_time in sorted_list:
                        if request_id < self.owner.id and self.token[request_id] < request_time:
                            self.state = NO_TOKEN
                            self.token[self.owner.id] = self.time
                            self.peer_list.peer(request_id).obtain_token(self._prepare(self.token))
                            self.token = None
                            break
                        elif request_id >= self.owner.id:
                            break
                print("4")
        finally:
            self.peer_list.lock.release()

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        #
        # Your code here.
        #
        # called when some other peer requests the token from
        # the current peer (should the current one have the token or not).
        print("deadlock?")
        self.peer_list.lock.acquire()
        try:
            print("No")
            # add the request to the list, potential risk of simultanous access, needs to be locked
            # Updates the time if needed (if the request time is greater than the internal time)
            self.time = max(self.time, time)
            # Updates the request list if needed with the new time
            self.request[pid] = max(self.request[pid], time)

        finally:
            self.peer_list.lock.release()

        if self.state == TOKEN_PRESENT:
            self.release()


    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        #
        # Your code here.
        #
        print(token)
        self.peer_list.lock.acquire()
        try:
            print("locked?")
            self.token = self._unprepare(token)
            print("after acquire")
            self.state = TOKEN_PRESENT
        finally:
            self.peer_list.lock.release()
            print("obtain_token(): Released lock")

    def display_status(self):
        """Print the status of this peer."""
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
