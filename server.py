#!/usr/bin/python3.4
import socket
from threading import Thread
import sqlite3
import sys, os, json

try:
    os.remove('metsa.db')
except:
    pass
conn = False
c = False
try:
    conn = sqlite3.connect('metsa.db')
    c = conn.cursor()
except Exception as e:
    print(e)
    sys.exit(1)
port = 10009
try:
    c.execute('create table persons (name text unique, pw text, id integer primary key autoincrement, lat real, lon real)')
    c.execute('create table groups (name text unique, pw text, owner integer, id integer primary key autoincrement, foreign key(owner) references person(id))')        
    c.execute('create table part (group_id integer, person_id integer, id integer primary key autoincrement, foreign key(group_id) references groups(id), foreign key(person_id) references persons(id))')
    c.execute('create table marks (txt text, group_id integer, person_id integer, type integer, lat real, lon real, id integer primary key autoincrement, foreign key(person_id) references persons(id), foreign key(group_id) references groups(id))')

    c.execute("insert into persons (name, pw, lat, lon) values (?, ?, ?, ?)", ('q', 'q', 45, 5.2))
    c.execute("insert into persons (name, pw, lat, lon) values (?, ?, ?, ?)", ('w', 'w', 50, 3.5))
    c.execute("insert into persons (name, pw, lat, lon) values (?, ?, ?, ?)", ('c', 'c', 20, 10))    
    c.execute("insert into groups (name, pw) values (?, ?)", ('f', 'f'))
    c.execute('insert into part (group_id, person_id) values(?, ?)', (1, 1))
    c.execute('insert into part (group_id, person_id) values(?, ?)', (1, 2))
    c.execute('insert into part (group_id, person_id) values(?, ?)', (1, 3))            
    c.execute("insert into marks (txt, group_id, person_id, type, lat, lon) values (?, ?,?, ?, ?, ?)", ('', 1, 1, 1, 10, 10))
    conn.commit()
except Exception as e:
    print(e)
    pass
conn.close()

def dbg(v):
    sys.stdout.write('[DBG] ')
    for m in v:
        sys.stdout.write(str(m) + ' ')
    print('')

def cdb():
    try:
        return sqlite3.connect('metsa.db')

    except:
        return False

def exe(c, sql, params=False):
    ret = False
    ok = True
    m = ''
    try:
        if params:
            ret = c.execute(sql, params)
        else:
            ret = c.execute(sql)
        c.commit()
    except Exception as e:
        c.rollback()
        ok = False
        m = 'exe error: ' + str(e)
    return (ok, ret, m)
        

def exists(c, tbl, f, v, p=False):
    try:
        args = (v, p) if p else (v,)
        end = ', pw = ? ' if p else ''
        stat, cur, m = exe(c, 'select count(*) from ' + tbl + ' where ' + f + ' = ? ' + end, args)
        if cur:
            row = cur.fetchone()
            if row and row[0] > 0:
                return 1
        return 0
    except Exception as e:
        print('exists ' + str(e))
        return -1

    
def numret(v):
    if v == -1:
        return (False, False, '')
    if v == 0:
        return (True, False, '')
    if v == 1:
        return (True, True, '')
    
def group_exists(c, n, p=False):
    return exists(c, 'groups', 'name', n, p)

def person_exists(c, n, p=False):
    return exists(c, 'persons', 'name', n, p)

def get_id(c, tbl, v, f='name', p=False):
    try:
        args = (v, p) if p else (v,)
        end = 'and pw = ? ' if p else ''
        sql = 'select id from ' + tbl + ' where ' + f + ' = ? ' + end        
        stat, cur, m = exe(c, sql, args)
        dbg(['get_id', sql, stat, cur, m])
        if cur:
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
        return 0
    except Exception as e:
        print('exists ' + str(e))
        return -1
    

def group_id(c, g, p = False):
    return get_id(c, 'groups', g, 'name', p)

def person_id(c, g, p= False):
    return get_id(c, 'persons', g, 'name', p)

def rete(m):
    return (False, False, m)

def rets(m=""):
    return (True, True, m)

def retf(m=""):
    return (True, False, m)

def group_add(c, n, pid, p=""):
    if not n:
            return retf('no group name')
    return exe(c, 'insert into groups (name, pw, owner) values (?, ?, ?)', (n, p, pid))
    
def group_del(c, n, pid):
    if not n:
        return retf('no group name')
    v = group_exists(c, n)
    if v == -1:
        return rete('db error')
    if v == 0:
        return retf('no group with that name')
    stat, ret, m = exe(c, 'select count(*) from groups where owner = ? and name = ?', (pid, n))
    if not stat:
        return rete(m)
    if not ret:
        return (True, False, 'operation failed')
    row = ret.fetchone()
    if not row or row[0] == 0:
        return (True, False, 'not group owner')
    stat, ret, m = exe(c, 'delete from groups where name = ?', (n,))
    if not stat:
        return rete(m)
    return rets()

def groups(c):
    stat, ret, m = exe(c, 'select * from groups')
    if not stat:
        return rete(m)
    if not ret:
        return (True, [], '')
    rows = []
    for r in ret:
        rows.append(r)
    return (True, rows, '')

def person_add(c, n, p=""):
    if not n:
        return retf('no name')
    v = person_exists(c, n)
    if v == -1:
        return rete('db error')
    if v == 1:
        return retf('name already exists')
    val = exe(c, 'insert into persons (name, pw, lat, lon) values (?, ?, 0, 0)', (n, p))
    dbg(['person_add'] + list(val))
    return val
    
def person_del(c, n):
    if not n:
        return (True, False, 'no person name')
    v = person_exists(c, n)
    if v == -1:
        return rete('db error')
    if v == 0:
        return retf('name does not exists')
    return exe(c, 'delete from persons where name = ?', (n,))

def persons(c):
    stat, ret, m = exe(c, 'select * from persons')
    if not stat:
        return (False, False, m)
    if not ret:
        return (True, [], '')
    rows = []
    for r in ret:
        rows.append(r)
    return (True, rows, '')


def group_join(c, g, pid, p=""):
    gid = group_id(c, g, p)
    if gid == -1:
        return rete('db error')
    if gid == 0:        
        return retf("group does not exists / wrong password")
    stat, ret, m = exe(c, 'select count(*) from part where group_id = ? and person_id = ?', (gid, pid))
    if not stat or not ret:
        return rete('db error')
    v = ret.fetchone()
    if v[0] > 0:
        return retf('already in the group')
    return exe(c, 'insert into part (group_id, person_id) values(?, ?)', (gid, pid))


def group_part(c, g, pid):
    gid = group_id(c, g)
    if gid == -1:
        return rete('db error')
    if gid == 0:
        return retf('group does not exists name :' + g + ' gid: ' + str(gid))
    return exe(c, 'delete from part where group_id = ? and person_id = ?', (gid, pid))
    
def person_groups(c, n):
    stat, ret, m = exe(c, """
    select g.*
    from persons p 
    inner join part on p.id = part.person_id
    inner join groups g on g.id = part.group_id
    where p.name = ?
    """, (n,))
    rows = []
    if stat and ret:
        for r in ret:
            rows.append(r)
    return (stat, rows, m)

def person_data(c, i):
    stat, ret, m = exe(c, """
    select 
    p.id as person_id, p.name as person_name, p.lat as person_lat, p.lon as person_lon,
    g.id as group_id, g.name as group_name
    from persons p
    inner join part on p.id = part.person_id
    inner join groups g on part.group_id = g.id    
    where part.group_id in (select group_id from part where person_id = ?)
    order by g.id
    """, (i,))
    r = {}
    if stat and ret:
        for tmp in ret:
            pid, pn, lat, lon, gid, gn = tmp
            if not gid in r.keys():
                r[gid] = { 'persons' : [], 'marks' : [] }
            r[gid]['name'] = gn
            r[gid]['persons'].append({'pid' : pid, 'name' : pn, 'lat' : lat, 'lon' : lon })


    ids = ",".join(list(map(lambda x: str(x), list(r.keys()))))
    stat, ret, m = exe(c, """
    select g.id as group_id, m.id as mark_id, m.lat as mark_lat, m.lon as mark_lon, m.type as mark_type, m.txt as mark_msg
    from groups g
    inner join marks m on m.group_id = g.id
    where g.id in (%s)
    """ % (ids))
    if stat and ret:
        for tmp in ret:
            gid, mid, lat, lon, tp, msg = tmp
            r[gid]['marks'].append({
                'lat' : lat,
                'lon' : lon,
                'type' : tp,
                'id' : mid,
                'msg' : msg
            })
    dbg(['person_data', stat, r, ret, m])
    return (stat, r if ret else False, m)

def group_persons(c, g):
    stat, ret, m = exe(c, """
    select p.*
    from groups g 
    inner join part on g.id = part.group_id
    inner join persons p on p.id = part.person_id
    where g.name = ?
    """, (n,))
    rows = []
    if stat and ret:
        for r in ret:
            rows.append(r)
    return (stat, rows, m)


def person_update(c, i, lat, lon):
    stat, ret, m = exe(c, 'update persons set lat = ?, lon = ? where id = ?', (lat, lon, i))
    return (stat, True, m)

def mark_add(c, gid, pid, lat, lon, tp, m=""):
    stat, ret, m = exe(c, 'insert into marks (group_id, person_id, type,  lat, lon, txt) values (?, ?, ?, ?, ?, ?)', (gid, pid, tp, lat, lon, m))
    if not stat:
        return (False, False, m)
    if not ret:
        return (True, False, 'virhe')
    return (True, True, '')

def mark_del(c, i):
    stat, ret, m = exe(c, 'delete from marks where id = ?', (i,))
    if not stat or not ret:
        return (False, False, m)
    return (True, True, '')




def cmsg(stat, m):
    return json.dumps([stat, m])
def msge(m):
    return json.dumps([0, m])
def msgs(m):
    return json.dumps([1, m])

def cmsge(m):
    return (True, msge(m))
def cmsgs(m):
    return (True, msgs(m))

def smsge(m):
    return (False, msge(m))
def smsgs(m):
    return (False, msgs(m))


def talk(s, m):
    c = s.con
    q = json.loads(m)
    if not q:
        return cmsge('invalid command')
    cmd = ''
    p0 = ''
    p1 = ''
    try:
        cmd = q[0]
        p0 = q[1]
        if len(q) > 2:
            p1 = q[2]
    except:
        return cmsge('invalid command')
    dbg(['talk', 'cmd', cmd, 'p0', p0, 'p1', p1])
    if cmd == 'ping':
        return cmsgs('pong')

    elif cmd == 'reg':
        stat, ret, m = person_add(c, p0, p1)
        if not stat or not ret:
            return cmsge(m)
        return cmsgs('')
            
    elif cmd == 'login':
        s.pid = -1
        s.name = ''
        s.pw = ''
        pid = person_id(c, p0, p1)
        if pid == -1:
            return cmsge('login: db error')
        if pid == 0:
            return cmsge('user name does not exists / wrong password')            
        s.pid = pid
        s.name = p0
        s.pw = p1
        return cmsgs('')

    elif cmd == 'logoff':
        s.pid = -1
        s.name = ''
        s.pw = ''
        return cmsgs('')
    
    elif cmd == 'data':
        if s.pid > 0:
            stat, ret, m = person_data(c, s.pid)
            if not stat:
                return cmsge(m)
            return cmsgs(ret)
        return cmsge('not logged in')
    elif cmd == 'update':
        if s.pid > 0:
            stat, ret, m = person_update(c, s.pid, p0, p1)
            if not stat:
                cmsge(m)
            return cmsgs('')
        return cmsge('not logged in')
    elif cmd == 'join':
        stat, ret, m = group_join(c, p0, s.pid, p1)
        if not stat or not ret:
            return (True, cmsg(0, m))
        return (True, cmsg(1, m))
    elif cmd == 'part':
        stat, ret, m = group_part(c, p0, s.pid)
        if not stat or not ret:
            return (True, cmsg(0, m))
        return (True, cmsg(1, ''))
    elif cmd == 'group_add':
        stat, ret, m = group_add(c, p0, s.pid, p1)
        if not stat or not ret:
            return (True, cmsg(0, m))
        return (True, cmsg(1, ''))
    elif cmd == 'group_del':
        stat, ret, m = group_del(c, p0, s.pid)
        if not stat or not ret:
            return (True, cmsg(0, m))
        return (True, cmsg(1, ''))
    elif cmd == 'mark_add':
        if len(q) < 4:
            return (True, cmsg(0, 'wrong parameter count'))
        gid = q[1]
        lat = q[2]
        lon = q[3]
        msg = ''
        if len(q) > 4:
            msg = q[4]
        stat, ret ,m = mark_add(c, gid, s.pid, lat, lon, 1, msg)
        print("mark_add " + str(stat) + " " + str(ret) + ' ' + str(m))
        if not stat or not ret:
            return (True, cmsg(0, m))
        return (True, cmsg(1, ''))    
    elif cmd == 'mark_del':
        stat, ret, m = mark_del(c, p0)
        if not stat or not ret:
            return (True, (cmsg(0, m)))
        return (True, cmsg(1, ''))
    
    
    elif cmd == 'close':
        return (False, '')    
    else:
        return (True, cmsg(0, '"invalid command"'))
    
    
def con_end(c, m):    
    swrite(c, cmsg(-1, m))

def stop(s):
    print('stop ' + str(s.name))
    try:
        s.c.close()
        s.con.close()
    except:
        pass

    
class Con(Thread):
    def __init__(s, c, a):
        super(Con, s).__init__()
        s.c = c
        s.a = a
        s.con = False
        s.pid = -1
        s.name = ''
        s.start()
    def run(s):
        s.con = cdb()
        if not s.con:
            con_end(s.c, 'Internal database error')
            return
        while True:
            try:
                print('')
                m = sread(s.c)
                if not m:
                    stop(s)
                    return                
                cont, msg = talk(s, m)
#                dbg(['cont', cont, 'msg', msg])
                if not cont:
                    stop(s)
                    return
                ret = swrite(s.c, msg)
                if not ret:
                    stop(s)
                    return
            except Exception as e:
                dbg(['exception', str(e), str(s.a)])
                stop(s)
                return            
        
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
    

def new(c, a):
    print('new ' + str(a))
    Con(c, a)
    
with socket.socket() as s:
    s.bind(('127.0.0.1', port))
    s.listen(5)
    while True:
        c, a = s.accept()
        c.settimeout(10)
        new(c, a)
    
