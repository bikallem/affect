import sys, re, collections
names = {1:'CALL',2:'BLOCK',4:'UNBLOCK',5:'CONT',14:'FDADD',20:'SELSTART',21:'SELREADY',22:'FDUNBLK',23:'SNAPSHOT'}
def load(path):
    evs=[]
    for line in open(path):
        if line.startswith('#'): continue
        seq,kind,dom,a,b,c = map(int,line.split())
        evs.append((seq,kind,dom,a,b,c))
    return evs
def fmt(e):
    seq,kind,dom,a,b,c = e
    n = names.get(kind,str(kind))
    if kind==22: return f"{seq:6} d{dom} {n} fd={a} blocked={b} sel={c//2} ours={c%2}"
    if kind==14: return f"{seq:6} d{dom} {n} fd={a} blocked={b}"
    if kind==2: return f"{seq:6} d{dom} {n} call={a} blocked={b} prims={c}"
    if kind==20: return f"{seq:6} d{dom} {n} sel={a} blocking={b}"
    if kind==23: return f"{seq:6} d{dom} {n} sel={a} nfds={b} first={c}"
    if kind==21: return f"{seq:6} d{dom} {n} sel={a} fd={b}"
    if kind==1: return f"{seq:6} d{dom} {n} parent={a} child={b}"
    return f"{seq:6} d{dom} {n} call={a} dom={b}"
out, dbg = sys.argv[1], sys.argv[1]+'.dbg'
m = re.findall(r'call=(\d+)', open(out).read())
evs = load(dbg)
for x in map(int, m):
    print(f"== stuck call {x}")
    blocks = [e for e in evs if e[1]==2 and e[3]==x]
    bids = {e[4] for e in blocks}
    fds = {e[3] for e in evs if e[1]==14 and e[4] in bids}
    print("   blocked ids:", sorted(bids), "fds:", fds)
    sels = {e[5]//2 for e in evs if e[1]==22 and e[4] in bids}
    rel = [e for e in evs if (e[1] in (2,4,5) and e[3]==x) or (e[1]==1 and e[3]==x)
           or (e[1] in (14,22) and (e[4] in bids or e[3] in fds))
           or (e[1] in (20,21,23) and e[3] in sels)
           or (e[1]==21 and e[4] in fds)]
    for e in rel: print("  ", fmt(e))
