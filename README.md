To run a challenge in Maelstrom `cd` into the Maelstrom directory:
`cd ~/Documents/Projects/maelstrom`
then run your Golang module:
Challenge 3:
`./maelstrom test -w broadcast --bin ~/Documents/Projects/gossip_glomers/challenge-3/challenge-3 --time-limit 20 --rate 100 --node-count 25 --latency 100`

Challenge 4:
`./maelstrom test -w g-counter --bin ~/Documents/Projects/gossip_glomers/challenge-4/challenge-4 --time-limit 20 --rate 100 --node-count 3 --nemesis partition`

Challenge 6:
a: `./maelstrom test -w txn-rw-register --bin ~/Documents/Projects/gossip_glomers/challenge-6/challenge-6 --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total`

b: `./maelstrom test -w txn-rw-register --bin ~/Documents/Projects/gossip_glomers/challenge-6/challenge-6 --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition`

