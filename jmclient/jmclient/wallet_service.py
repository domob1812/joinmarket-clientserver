#! /usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *

from twisted.internet import reactor
from twisted.internet import task
from twisted.application.service import Service
import copy
from numbers import Integral
from jmclient.configure import jm_single, get_log
import jmbitcoin as btc

"""Wallet service

The purpose of this independent service is to allow
running applications to keep an up to date, asynchronous
view of the current state of its wallet, deferring any
polling mechanisms needed against the backend blockchain
interface here.
"""

jlog = get_log()

class WalletService(Service):

    def __init__(self, wallet):
        # The two principal member variables
        # are the blockchaininterface instance,
        # which is currently global in JM but
        # could be more flexible in future, and
        # the JM wallet object.
        # TODO support multiple wallets here.
        self.bci = None
        self.wallet = wallet
        self.synced = False

        # Lists of registered callbacks for events
        # on transactions. Note these functions
        # must process any transaction and filter
        # on e.g. txid or address themselves.
        self.unconfirmed_callbacks = []
        self.confirmed_callbacks = []

        # transactions we are actively monitoring,
        # i.e. they are not new but we want to track:
        self.active_txids = []

    def startService(self):
        """ Encapsulates start up actions.
        Here wallet sync.
        """
        Service.startService(self)
        self.bci = jm_single().bc_interface
        self.request_sync_wallet()

    def stopService(self):
        """ Encapsulates shut down actions.
        Here shut down main tx monitoring loop.
        """
        self.monitor_loop.stop()
        Service.stopService(self)

    def request_sync_wallet(self):
        """ Ensures wallet sync is complete
        before the main event loop starts.
        """
        d = task.deferLater(reactor, 0.0, self.sync_wallet)
        d.addCallback(self.start_wallet_monitoring)

    def sync_wallet(self):
        """ Syncs wallet using fast sync.
        TODO: enable slow sync as option here.
        Before starting the event loop, we cache
        the current most recent transactions as
        reported by the blockchain interface, since
        we are interested in deltas.
        """
        self.bci.sync_wallet(self.wallet, fast=True)
        # TODO handle restart callback etc.
        self.synced = True
        # Don't attempt updates on transactions that existed
        # before startup
        self.old_txs = self.bci.list_transactions(100)
        return self.synced

    def register_callbacks(self, callbacks, unconfirmed=True):
        """ Register callbacks that will be called by the
        transaction monitor loop, assuming new transactions that
        appear do affect the wallet at all.
        Callback arguments are currently (txd, txid) and return
        is boolean.
        Note that callbacks MUST correctly return True if they
        recognized the transaction and processed it, and False
        if not. The True return value will be used to remove
        the callback from the list.
        """
        if unconfirmed:
            self.unconfirmed_callbacks.extend(callbacks)
        else:
            self.confirmed_callbacks.extend(callbacks)

    def start_wallet_monitoring(self, syncresult):
        """ Once the initialization of the service
        (currently, means: wallet sync) is complete,
        we start the main monitoring jobs of the
        wallet service (currently, means: monitoring
        all new transactions on the blockchain that
        are recognised as belonging to the Bitcoin
        Core wallet).
        """
        if not syncresult:
            jlog.error("Failed to sync the bitcoin wallet. Shutting down.")
            reactor.stop()
        jlog.info("Starting transaction monitor in walletservice")
        self.monitor_loop = task.LoopingCall(
            self.transaction_monitor)
        self.monitor_loop.start(5.0)

    def transaction_monitor(self):
        """Keeps track of any changes in the wallet (new transactions).
        Intended to be run as a twisted task.LoopingCall so that this
        Service is constantly in near-realtime sync with the blockchain.
        """
        txlist = self.bci.list_transactions(100)
        new_txs = []
        for x in txlist:
            if x['txid'] in self.active_txids or x['txid'] not in self.old_txs:
                new_txs.append(x)
        # reset for next polling event:
        self.old_txs = [x['txid'] for x in txlist]

        for tx in new_txs:
            # filter on this Joinmarket wallet:
            if "label" not in tx.keys() or \
               tx["label"] != self.bci.get_wallet_name(self.wallet):
                continue
            res = self.bci.get_transaction(tx["txid"])
            if not res:
                continue
            txd = self.bci.get_deser_from_gettransaction(res)
            if txd is None:
                continue
            confs = res["confirmations"]
            if not isinstance(confs, Integral):
                jlog.warning("Malformed gettx result: " + str(res))
                continue
            txid = btc.txhash(btc.serialize(txd))
            if confs < 0:
                jlog.info("Transaction: " + txid + " has a conflict, abandoning.")
                continue

            removed_utxos, added_utxos = self.wallet.process_new_tx(txd, txid)

            # note that len(added_utxos) > 0 is not a sufficient condition for
            # the tx being new, since wallet.add_new_utxos will happily re-add
            # a utxo that already exists; but this does not cause re-firing
            # of callbacks since we in all cases delete the callback after being
            # called once.
            # Note also that it's entirely possible that there are only removals,
            # not additions, to the utxo set e.g. in tumbler sweeps or direct
            # send payments.
            if len(added_utxos) > 0 or len(removed_utxos) > 0:
                if confs == 0:
                    for f in self.unconfirmed_callbacks:
                        # only keep this callback if it was not successful
                        if f(txd, txid):
                            self.unconfirmed_callbacks.remove(f)
                            self.active_txids.append(txid)
                else:
                    for f in self.confirmed_callbacks:
                        if f(txd, txid):
                            self.confirmed_callbacks.remove(f)
                            if txid in self.active_txids:
                                self.active_txids.remove(txid)
