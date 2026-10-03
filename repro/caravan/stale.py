import sys
for path in sys.argv[1:]:
    evs=[tuple(map(int,l.split())) for l in open(path) if not l.startswith('#')]
    snap={e[3]:e[0] for e in evs if e[1]==23}
    add={e[4]:e for e in evs if e[1]==14}
    blk={e[4]:e for e in evs if e[1]==2}
    stale=[]
    for e in evs:
        if e[1]==22 and e[5]%2==1:
            sel=e[5]//2; b=e[4]
            if b in add and snap.get(sel,1<<60) < add[b][0]:
                call = blk[b][3] if b in blk else None
                stale.append((e[0], e[3], b, sel, snap[sel], add[b][0], call))
    print(path, "stale wakeups:", len(stale))
    for s in stale: print("   seq %d fd=%d blocked=%d sel=%d snapshot@%d registered@%d call=%s" % s)
