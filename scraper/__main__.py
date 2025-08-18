from scraper.scraper import run
import sys

if len(sys.argv) < 2:
    print("Usage: python -m scraper <output_filename> [--historical]")
    sys.exit(1)

output_file = sys.argv[1]
historical = '--historical' in sys.argv

run(output_file, historical)
