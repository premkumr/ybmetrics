#!/usr/bin/env python3
import argparse
import copy
import json
import os
import re
import shelve
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from collections import OrderedDict
import itertools
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import requests
except Exception as e:
    print("Please install requests via `pip install requests`")
    sys.exit(1)

try:
    from tabulate import tabulate
except Exception as e:
    print("Please install tabulate via `pip install tabulate`")
    sys.exit(1)


class BraceExpander:
    '''
        Expand :
            127.0.0.{1..3} ==> [127.0.0.1, 127.0.0.2, 127.0.0.3]
            127.0.0.{1,3} ==> [127.0.0.1, 127.0.0.3]
    '''
    def expand(self, s):
        if not self.has_braces(s):
            return [s]
        items = []
        for t in self.expand_one(s):
            if self.has_braces(t):
                items += self.expand(t)
            else:
                items.append(t)
        return items

    def has_braces(self, s):
        if '{' not in s or '}' not in s:
            return False
        start, end = self.brace_indexes(s)
        return start < end

    def expand_one(self, s):
        start, end = self.brace_indexes(s)
        before, after = s[:start], s[end+1:]
        inside_parts = s[start+1:end].split(',')
        if len(inside_parts) == 1 and '..' in inside_parts[0]:
            # do a range
            start, end = list(map(int, inside_parts[0].split('..')))
            inside_parts = list(map(str, list(range(start, end+1))))
        return (before + inside + after for inside in inside_parts)

    def brace_indexes(self, s):
        return s.index('{'), s.index('}')
    
    def test(self):
        assert(self.expand('127.0.0.1') == ['127.0.0.1'])
        assert(self.expand('127.0.0.{1..3}') == ['127.0.0.1', '127.0.0.2', '127.0.0.3'])
        assert(self.expand('127.0.0.{1,3}') == ['127.0.0.1', '127.0.0.3'])


class MetricsTracker:
    def __init__(self):
        dbfile = os.path.join(tempfile.gettempdir(), 'metrics.db')
        self._db = None
        try:
            self._db = shelve.open(dbfile, writeback=True)
        except Exception as e:
            print('Unable to open db.. Please check if there is another metrics process running!!!')
            sys.exit(1)
        self.keypattern = '.*'
        self.full_tabletid = False
        self.hosts = ['127.0.0.1:9000']
        if 'items' not in self._db:
            self._db['items'] = []
        self.db = self._db['items']
        del self.db[2:]
        self.print_count = 0
        self.failedhosts = set()
        
    def __del__(self):
        if self._db is not None:
            self._db.close()

    def clean(self):
        self.db=[]

    def get_metrics(self):
        clean_data = {}
        timestamp = time.time()
        
        for host in self.hosts:
            #print('fetching from : {}'.format(host))
            hostname = host
            if ':' not in hostname:
                hostname = '{}:{}'.format(host, 9000)
            url = 'http://{}/metrics'.format(hostname)
            try:
                response = requests.get(url, verify=False)
                if hostname in self.failedhosts:
                    print('back online : [{}]'.format(hostname))
                    self.failedhosts.remove(hostname)
            except requests.exceptions.ConnectionError as e:
                if hostname not in self.failedhosts:
                    print('unable to connnect to : [{}]'.format(hostname))
                    self.failedhosts.add(hostname)
                continue
            
            data = json.loads(response.text)

            # tablet data
            for tablet in data:
                if (tablet['type']=='tablet' and
                        tablet['attributes']['namespace_name'] != 'system' and
                        tablet['attributes']['table_name'] != 'write_read_test'):
                    tinfo = {}
                    tinfo['namespace_name'] = tablet['attributes']['namespace_name']
                    tinfo['table_name'] = tablet['attributes']['table_name']
                    tinfo['hostname'] = hostname
                    tinfo['metrics'] = {}
                    for m in tablet['metrics']:
                        if m['value'] > 0 :
                            tinfo['metrics'][m['name']] = m['value']

                    # get only the leaders
                    if tinfo['metrics'].get('is_raft_leader', 0) == 0: continue
                    clean_data[tablet['id']] = tinfo
        
        hash_code = hash(str(clean_data))
        print
        if len(self.db) > 0 :
            last_hash, _, _ = self.db[0]
            if last_hash == hash_code:
                # no change from last time
                return None

        self.db.insert(0, (hash_code, timestamp, clean_data))
        
        return clean_data

    def get_diff(self, current, last):
        if last is None:
            print('last is none')
            return current
        data = copy.deepcopy(current)
        for tid, tdata in current.items():
            tmetrics = tdata.setdefault('metrics', {})
            dmetrics = data[tid].setdefault('metrics', {})
            if tmetrics.get('is_raft_leader', 0) == 1 and tid in last:
                last_metrics = last[tid]['metrics']
                for k in tmetrics:
                    if k in last_metrics:
                        dmetrics.setdefault(k,0)
                        dmetrics[k] -= last_metrics[k]
                        # remove 0 & -ve values
                        if dmetrics[k] <= 0:
                            del dmetrics[k]
                    else:
                        dmetrics[k] = tmetrics[k]
                if len(dmetrics) == 0:
                    del data[tid]
            elif tmetrics.get('is_raft_leader', 0) == 1 and tid not in last:
                pass
            else:
                del data[tid]
        return data

    def display_data(self, data):
        self.print_count += 1
        error = ''
        
        if len(self.failedhosts) > 0 :
            error = ' : [unable to fetch from {} '.format(sorted(list(self.failedhosts)))

        print('>>> {}{}'.format(self.print_count, error))
        print(data)
        print("\n")

    def print_metrics(self, metrics, vertical=True, top = None):
        table=[]
        pattern = re.compile(self.keypattern)
        keyset = set()
        if vertical:
            for tid in metrics:
                metrics[tid].setdefault('metrics', {})
                for k,v in metrics[tid]['metrics'].items():
                    k = k.replace('rocksdb_number_','').replace('rocksdb_','')
                    if pattern.match(k):
                        keyset.add(k)
                        tablet_id = tid if self.full_tabletid else tid[:12] + '...'
                        table.append([k, v, tablet_id, metrics[tid]['table_name'], metrics[tid]['hostname']])
            if len(keyset) == 1 :
                # sort the table on this key
                k = keyset.pop()
                keyset.add(k)
                table.sort(key=lambda a : a[1], reverse=True)
                if top:
                    del table[top:]
            headers=('metric', 'value', 'tablet-id', 'table', 'host')
            align=('right','center', 'left', 'left')
            if len(table) > 0:
                self.display_data(tabulate(table, headers, tablefmt="presto", colalign=align))
        else:
            # default
            for tid in metrics:
                tablet_id = tid if self.full_tabletid else tid[:12] + '...'
                data = { 'tablet-id' : tablet_id, 'table': metrics[tid]['table_name'],'host' : metrics[tid]['hostname']}
                for k,v in metrics[tid]['metrics'].items():
                    k = k.replace('rocksdb_number_','').replace('rocksdb_','')
                    if pattern.match(k):
                        data[k] = v
                        keyset.add(k)
                table.append(data)
            
            table.sort(key=lambda a : a.get('table', ''))
            if len(keyset) == 1 :
                # sort the table on this key
                k = keyset.pop()
                keyset.add(k)
                table.sort(key=lambda a : a.get(k, 0), reverse=True)
                if top:
                    del table[top:]
            if len(table) > 0 and len(keyset) > 0:
                pre_headers = ['tablet-id', 'table', 'host']
                post_headers = sorted(list(keyset))
                
                # add the total row.
                totals = {}
                if len(table) > 1:
                    totals = OrderedDict.fromkeys(pre_headers + post_headers)
                    for k in post_headers:
                        totals[k] = sum([ row.get(k, 0)  for row in table])
                    max_host_len = max([ len(row.get('host', '')) for row in table])
                    totals['host'] = '>' * (max_host_len-8) + ' total ='

                headers = OrderedDict.fromkeys(pre_headers + post_headers)
                for k in headers:
                    headers[k] = k
                table.insert(0, headers)
                if len(totals) > 0:
                    table.append(totals)
                self.display_data(tabulate(table, tablefmt="presto", headers='firstrow'))

        
    def monitor(self, interval = 10, vertical = True, top=None):
        try:
            while True:
                current = self.get_metrics()
                if current is not None:
                    last = None
                    if len(self.db) > 1 :
                        last = self.db[1][2]
                    diff = self.get_diff(current, last)
                    if len(diff) > 0:
                        self.print_metrics(diff, vertical, top)
                else:
                    #print('no change')
                    pass
                self.sleep(interval)
        except KeyboardInterrupt:
            pass

    def sleep(self, interval) :
        for n in range(interval, 0, -1):
            progressbar = '-' * n + ' ' *(interval - n)
            print(progressbar, end='\r')
            time.sleep(1)

    def tablets(self):
        metrics = self.get_metrics()
        tablet_info = []
        for tid in metrics:
            tablet = metrics[tid]
            tablet.setdefault('metrics', {})
            table_name = metrics[tid]['table_name']
            tablet_info.append([ table_name, tid, tablet['metrics'].get('is_raft_leader',0)])
        print(tabulate(tablet_info, tablefmt="presto", headers=['table','tablet', 'leader']))


def cli(argv: Optional[str] = None):
    argv = argv or sys.argv[:]
    prog_name = Path(argv[0]).name
    parser = argparse.ArgumentParser(prog=prog_name, description='Metrics Monitor')
    parser.add_argument('-i', '--interval', dest='interval', type=int, default = 5,  help = 'time to wait')
    parser.add_argument('--top', dest='top', type = int, default = None,  help = 'top N tablet ids')

    parser.add_argument('-v', '--vertical', dest='vertical', default = False, action='store_true')
    parser.add_argument('--no-vertical', dest='vertical', default = False, action='store_false')
    parser.add_argument('--full-tabletid', dest='full_tabletid', default = False, action='store_true', help='print full tablet id')

    parser.add_argument('-k', '--keys', dest='keys', default = '(rows_inserted|db_seek|db_next)$', help='Key pattern(regex)')
    parser.add_argument('--rwkeys', default = False, action='store_true', help='only rocks r/w keys')
    parser.add_argument('--read', default = False, action='store_true', help='only rocks read key')
    parser.add_argument('--write', default = False, action='store_true', help='only rocks write key')
    parser.add_argument('--txn', default = False, action='store_true', help='only txn keys')

    parser.add_argument('--host', dest='hosts', action='append', default=[], help = 'tserver hosts (host:port[9000])')
    parser.add_argument('-m', '--mode', dest='mode', choices=['monitor', 'tablets','clean'], default='monitor', nargs='?', help = 'Execution mode')
    
    args = parser.parse_args()
  
    if args.rwkeys:
        args.keys = '(rows_inserted|db_seek)$'
    elif args.read:
        args.keys = '(db_seek)$'
    elif args.write:
        args.keys = '(rows_inserted)$'
    elif args.txn:
        args.keys = '(transaction)'

    if len(args.hosts) == 0:
        if type(args.hosts) != list:
            args.hosts = []
        args.hosts.append('127.0.0.{1..3}')
    elif type(args.hosts) == str:
        args.hosts = [args.hosts]

    m = MetricsTracker()
    m.hosts = []
    expander = BraceExpander()

    m.hosts = list(itertools.chain.from_iterable([expander.expand(h) for h in args.hosts]))
    
    m.keypattern = args.keys
    m.full_tabletid = args.full_tabletid
    if args.mode == 'monitor':
        m.monitor(args.interval, args.vertical, args.top)
    elif args.mode == 'tablets':
        m.tablets()
    elif args.mode == 'clean':
        m.clean()

if __name__ == "__main__":
    cli()
    
