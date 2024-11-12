# If you are new to Makefiles: https://makefiletutorial.com

PAPER := output/paper.pdf
PRESENTATION := output/presentation.pdf

TARGETS :=  $(PAPER) $(PRESENTATION)

# Configs
PULL_DATA_CFG := config/pull_data_cfg.yaml
PREPARE_DATA_CFG := config/prepare_data_cfg.yaml
DO_ANALYSIS_CFG := config/do_analysis_cfg.yaml

PULLED_DATA := data/pulled/financial_data.csv
PREPARED_DATA := data/generated/financial_data_prepared.csv
TABLE_1 := data/generated/table_1.pickle
RESULTS := output/em_results.pickle

.PHONY: all clean very-clean dist-clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) $(RESULTS) $(PREPARED_DATA) $(TABLE_1)

very-clean: clean
	rm -f $(PULLED_DATA)

dist-clean: very-clean
	rm -f config.csv

$(PULLED_DATA): code/python/pull_wrds_data.py $(PULL_DATA_CFG)
	python3 $<

$(PREPARED_DATA): code/python/prepare_data.py $(PULLED_DATA) \
	$(PREPARE_DATA_CFG)
	python3 $<

$(RESULTS): code/python/do_analysis.py $(PREPARED_DATA) \
	$(DO_ANALYSIS_CFG)
	python3 $<

$(PAPER): doc/paper.qmd doc/references.bib $(RESULTS)
	quarto render $< --quiet
	mv doc/paper.pdf output
	rm -f doc/paper.ttt doc/paper.fff

$(PRESENTATION): doc/presentation.qmd $(RESULTS) \
	doc/beamer_theme_trr266.sty
	quarto render $< --quiet
	mv doc/presentation.pdf output
	rm -rf doc/presentation_files