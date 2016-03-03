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

        # Should not need to lock when instantiating ourself in the request list
        self.request[self.owner.id] = 0

        self.peer_list.lock.aquire()
        # Try catch to make sure we always release the global peer list lock
        try:
            # Get the peer list with only the IDs
            peers = self.peer_list.get_peers()[0]
            #print(peers)
            # Instantiate the request list for peers from the peer list
            for peer_id in peers:
                self.request[peer_id] = 0
            # if the list is empty or I have the smallest ID, I get to start with the token
            if len(peers) == 0 or self.owner.id > min(peers):
                self.token = {self.owner.id: self.time}
                self.state = TOKEN_PRESENT
        finally:
            #print("done")
            self.peer_list.lock.release()
        pass

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        #
        # Your code here.
        #
        #unregister_peer(self.owner.id)
        if self.state == TOKEN_HELD:
            self.release()
        
        self.peer_list.lock.acquire()
        try:
            peers = self.peer_list.get_peers()[0]
            # If there exist other peers and we have the token
            if len(peers) > 0 and self.state == TOKEN_PRESENT:
                # Note: Hmm, what do we do if the peer we want to give the token to died? Loop the list?
                self.peer_list.peer(min(peers)).obtain_token(self.token)

        finally:
            self.peer_list.lock.release()
        pass

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        #
        # Your code here.
        #

        
        self.peer_list.lock.acquire()
        try:
            # add the calling peer to the request list
            self.request[pid] = 0
            # peers = self.peer_list.get_peers()[0]
            # no key present?
            # pid is lowest
            # peer has token? possible?
            
            
        finally:
            self.peer_list.lock.release()
        pass

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            self.request.pop(pid,None)
        finally:
            self.peer_list.lock.release()
        pass

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
            peers = self.peer_list.get_peers()[0]
            if len(peers) > 0 and self.state == NO_TOKEN:
                self.time += 1
                self.request[self.owner.id] = self.time
            
                for peer_id in peers:
                    if peer_id != self.owner.id:
                        self.peer_list.peer(peer_id).request_token(self.time, self.owner.id)
                    
                # Try to acquire the lock, will be waiting until another peer release the lock
                # This assumes that locks are implemented in a way that allows shared locks
                self.peer_list.lock.acquire()
        finally:
            self.peer_list.lock.release()
        self.peer_list.lock.acquire()
        self.state = TOKEN_HELD
        self.peer_list.lock.release()
        pass

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        #
        # Your code here.
        #
        #called when the current peer releases the lock; if there are
        #peers waiting for the token, the current peer should pass the token to one
        #of them (carefully think through to whom in order to ensure fairness).
        self.request[self.owner.id] = 0
        if max(self.request) > 0: 
            for request_id,request_time in self.request.items():
                if request_time == min(self.request):
                    candidates = [candidates, request_id]
        
            self.state = NO_TOKEN
            self.peer_list.peer(min(candidates)).obtain_token(self.token) 
        else:
            self.state = TOKEN_PRESENT
        pass

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        #
        # Your code here.
        #
        # called when some other peer requests the token from
        # the current peer (should the current one have the token or not).
        self.peer_list.lock.acquire()
        try:
            # add the request to the list, potential risk of simultanous access, needs to be locked
            self.request[pid] = time
            if self.state == TOKEN_PRESENT:
                self.peer_list.peer(pid).
        finally:
            self.peer_list.lock.release()
        pass

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        #
        # Your code here.
        #

        self.peer_list.lock.acquire()
        try:


        finally:
            self.peer_list.lock.release()
        pass

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
