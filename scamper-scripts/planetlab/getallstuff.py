import os

ds = set()
dname = 'ext/www/rr_data/coverage/csv_echo_replies/'
for fname in os.listdir(dname):
    with open(os.path.join(dname, fname)) as f:
        for line in f:
            chunks = line.strip().split(',')
            for d in chunks[2:]:
                if d not in ds:
                    print(d)
                    ds.add(d)
