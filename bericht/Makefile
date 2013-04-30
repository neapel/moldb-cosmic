bericht.pdf : bericht.tex
	lualatex $^
	lualatex $^

.PHONY : clean
clean :
	-rm bericht.pdf *.aux *.log *.out *.toc

watch :
	latexmk -f -pdf -pdflatex=lualatex -pvc bericht
