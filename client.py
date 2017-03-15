#!/usr/bin/python3.4
import socket, sys, time, json
from threading import Thread, Lock


def sr(c, size):
    ret = []
    rec = 0
    while rec < size:
        b = c.recv(min(size - rec, 4096))
        if b == b'':
            return []
        ret.append(b)
        rec += len(b)
    return b''.join(ret).decode()

def sread(c):
    size = sr(c, 64)
    if not size:
        return ''
    return sr(c, int(size))
    
def swrite(c, msg):
    b = msg.encode()
    l = len(b)
    sent = 0
    hm = str(l).zfill(64).encode()
    while sent < 64:
        s = c.send(hm[sent:])
        if s == 0:
            return False
        sent += s
    sent = 0
    while sent < l:
        s = c.send(b[sent:])
        if s == 0:
            return False
        sent += s
    return True

class Soc(Thread):
    def __init__(s, done, msg_fn):
        super(Soc, s).__init__()
        s.l = Lock()
        s.ip = '127.0.0.1'
        s.port = 10009                    
        s.end = True
        s.daemon = True
        s.que = []
        s.end_fn = done
        s.err = ''
        s.done = True
        s.msg = msg_fn
        s.c = socket.socket()
        try:
            s.c.connect((s.ip, s.port))
        except Exception as e:
            print(e)
            s.err = str(e)
        if not s.err:
            s.end = False
            s.done = False

    def test(s):
        return not s.done
        
    def cmd(s, args):
        with s.l:
            s.que.append(args)
    def cmd_pop(s):
        tmp = []
        with s.l:
            if s.que:
                tmp = s.que[0]                
                s.que = s.que[1:]
        return tmp
    def close(s):
        if s.done:
            return
        s.end = True
        while not s.done:
            pass
        return
            
    def run(s):
        s.done = False
        s.end = False
        n = 0
        while not s.end:
            try:
                tmp = s.cmd_pop()
                if not tmp:
                    if n == 20:
                        s.cmd([lambda stat, ret: dummy_call(),'ping', 'dummy'])
                        n = 0
                    else:
                        n += 1
                else:
                    n = 0
                    ret = tmp[0]
                    tmp = tmp[1:]
                    d = json.dumps(tmp)
                    #print('executing ' + str(tmp))
                    #print(d)
                    x = swrite(s.c, d)
                    if not x:
                        print('con write error')
                        s.msg(3, 'Error could not write')
                        s.end = True                        
                        continue
                    m = sread(s.c)
                    if not m:
                        s.end = True
                        continue
                    rmsg = s.parse(m)
                    if not rmsg:
                        s.msg(3, 'Error witch server command')
                    else:
                        num = int(rmsg[0])
                        tmpm = rmsg[1]
                        ret(num, tmpm)
            except Exception as e:
                s.err = str(e)
                s.end = True
            if not s.end:
                time.sleep(0.1)
        if s.c:
            try:
                s.c.close()
            except:
                pass
            s.c = False
        s.done = True
        s.end_fn(s.err)
        
    def parse(s, m):
        if not m:
            return []
        return json.loads(m)
        
class Con():
    def __init__(s, dc_fn, err_fn):
        s.name = 'Not logged in'
        s.pw = ''
        s.t = False
        s.logged = False
        s.dc_fn = dc_fn
        s.err_fn = err_fn
    def connect(s, fnok):
        m = ''
        if s.t and s.t.test():
            fnok()
            return
        if s.t:
            s.t.close()
        s.t = Soc(s.dc_fn, s.err_fn)
        if not s.t.test():
            return
        s.t.start()
        fnok()
        return
    
    def test(s):
        if not s.t:
            return False
        return s.t.test()
    def dc(s):
        if s.t:
            s.t.close()

    def cmd(s, fn, fne, args):
        if not s.test():
            fne('not connected')
            return        
        s.t.cmd([lambda stat, ret: fn(ret) if stat else fne(ret)] + args)
    def cmdl(s, fn, fne, args):
        if not s.test():
            fne('not connected')
            return
        if not s.logged:
            fne('not logged in')
            return
        s.t.cmd([lambda stat, ret: fn(ret) if stat else fne(ret)] + args)
    def ping(s, fn, fne):
        s.cmd(fn, fne, ['ping', 'dummy'])
    def reg(s, n, p, fn, fne):
        s.cmd(fn, fne, ['reg', n, p])
    def setinfo(s, n, p, l):
        s.name = n
        s.pw = p
        s.logged = l
    def login(s, n, p, fn, fne):
        s.setinfo('', '', False)
        s.cmd(lambda x: (s.setinfo(n, p, True), fn(x)),
              lambda x: (s.setinfo('Not logged in', '', False),  fne(x)), ['login', n, p])

    def logged_in(s):
        return s.logged
    
    def logoff(s, fn, fne):
        s.name = 'Not logged in'
        s.pw = ''
        s.logged = False
        s.cmd(fn, fne, ['logoff', 'dummy'])
    def data(s, fn, fne):
        s.cmdl(fn, fne, ['data', 'dummy'])
    def update(s, lat, lon, fn, fne):
        s.cmdl(fn, fne, ['update', lat, lon])
    def group_add(s, n, p, fn, fne):
        s.cmdl(fn, fne, ['group_add', n, p])
    def group_del(s, n, fn, fne):
        s.cmdl(fn, fne, ['group_del', n])
    def group_join(s, n, p, fn, fne):
        s.cmdl(fn, fne, ['join', n, p])
    def group_part(s, n, fn, fne):
        s.cmdl(fn, fne, ['part', n])
    def mark_add(s, gid, la, lo, m, fn, fne):
        s.cmdl(fn, fne, ['mark_add', gid, la, lo, m])
    def mark_del(s, mid, fn, fne):
        s.cmdl(fn, fne, ['mark_del', mid])
        
            
        
end = True        
def disconnect(m=""):
    print("disconnected")
    print(m)
    global end
    end = False
def error_msg(lvl, m=""):
    print('error msg')
    print(lvl)
    print(m)
    
def dummy_call():
    pass
    
#c = Con(disconnect, error_msg)   
#if not c.connect():
#    print('error')
#    print(c)
#if not c.test():
#    print('could not connect')


#    
#def pxs(x):
#    print('success ' + str(x))
#def pxe(x):
#    print('error ' + str(x))
#    
#c.reg('s', 's', pxs, pxe)
#time.sleep(2)
#print('')
#c.login('s', 's', pxs, pxe)
#time.sleep(2)
#print('')
#c.ping(pxs, pxe)
#time.sleep(2)
#print('')
#c.group_add('peenus', 'xoxoxo', pxs, pxe)            
# 
#time.sleep(2)
#print('')
#c.group_join('peenus', 'xoxoxo', pxs, pxe)
#time.sleep(2)
# 
#c.data(pxs, pxe)
#time.sleep(2)
#print('')
#c.group_part('peenus', pxs, pxe)
#time.sleep(2)
#print('')
#c.group_del('peenus', pxs, pxe)
#time.sleep(2)
#print('')
#c.logoff(lambda x: print('logoff'), lambda x: print('logoff failed'))
# 
#time.sleep(3)
#c.dc()
#while(c.test()):
#    pass
#while(end):
#    pass
