# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

SPHINXAPIDOC  = sphinx-apidoc
APIDOCFLAGS   = --separate --module-first
MODULEPATH    = ../cisticola
SOURCEFILES   = cisticola.*
MODULEFILE    = modules.rst

POST_APIDOC   = edit_apidoc.sh

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Custom process and flags for generating Sphinx sources
apidoc:
	rm $(SOURCEDIR)/$(SOURCEFILES)
	$(SPHINXAPIDOC) $(APIDOCFLAGS) -o "$(SOURCEDIR)" "$(MODULEPATH)"
	rm $(SOURCEDIR)/$(MODULEFILE)
	bash $(POST_APIDOC)
	

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
