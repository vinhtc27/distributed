serve:
	cd maelstrom && ./maelstrom serve

echo:
	make build && cd maelstrom && ./maelstrom test -w echo --bin ../target/release/echo --node-count 1 --time-limit 10

unique-ids:
	make build && cd maelstrom && ./maelstrom test -w unique-ids --bin ../target/release/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

single-node-broadcast:
	make build && cd maelstrom && ./maelstrom test -w broadcast --bin ../target/release/broadcast --node-count 1 --time-limit 20 --rate 10

multi-node-broadcast:
	make build && cd maelstrom && ./maelstrom test -w broadcast --bin ../target/release/broadcast --node-count 5 --time-limit 20 --rate 10

fault-tolerant-broadcast:
	make build && cd maelstrom && ./maelstrom test -w broadcast --bin ../target/release/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

efficient-broadcast:
	make build && cd maelstrom && ./maelstrom test -w broadcast --bin ../target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

grow-only-counter:
	make build && cd maelstrom && ./maelstrom test -w g-counter --bin ../target/release/g-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

pn-counter:
	make build && cd maelstrom && ./maelstrom test -w pn-counter --bin ../target/release/g-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

single-node-kafka:
	make build && cd maelstrom && ./maelstrom test -w kafka --bin ../target/release/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

multi-node-kafka:
	make build && cd maelstrom && ./maelstrom test -w kafka --bin ../target/release/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000