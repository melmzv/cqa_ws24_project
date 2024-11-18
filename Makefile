# If you are new to Makefiles: https://makefiletutorial.com

PAPER := output/paper.pdf
PICKLE := output/figure3_market_shares.pickle

TARGETS := $(PAPER) $(PICKLE)

# Configs
PULL_DATA_CFG := config/pull_data_cfg.yaml
PREPARE_DATA_CFG := config/prepare_data_cfg.yaml
DO_ANALYSIS_CFG := config/do_analysis_cfg.yaml

PULLED_DATA := data/pulled/transparency_report_data_2021.csv
PREPARED_DATA := data/generated/prepared_transparency_data.csv
RESULTS := output/aggregated_market_shares.csv

.PHONY: all clean very-clean dist-clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) $(RESULTS) $(PREPARED_DATA)

very-clean: clean
	rm -f $(PULLED_DATA)

dist-clean: very-clean
	rm -f config.csv

$(PULLED_DATA): code/python/pull_wrds_data.py $(PULL_DATA_CFG)
	python3 $<

$(PREPARED_DATA): code/python/prepare_data.py $(PULLED_DATA) \
	$(PREPARE_DATA_CFG)
	python3 $<

$(RESULTS) $(PICKLE): code/python/do_analysis.py $(PREPARED_DATA) \
	$(DO_ANALYSIS_CFG)
	python3 $<

$(PAPER): doc/paper.qmd doc/references.bib $(RESULTS) $(PICKLE)
	quarto render $< --quiet
	mv doc/paper.pdf output
	rm -f doc/paper.ttt doc/paper.fff