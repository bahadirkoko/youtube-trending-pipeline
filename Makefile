.PHONY: backfill backfill-dry set-secret

backfill:
	./scripts/backfill_kaggle.sh

backfill-dry:
	./scripts/backfill_kaggle.sh --dry-run

set-secret:
	./scripts/set_youtube_key.sh