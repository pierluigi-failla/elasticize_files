================
Elasticize Files
================


Crawl, process and index your files the way you like, applying in top of them the functions you like, storing the results in Elastic. Fast, in parellel.


Description
===========

Take a look to the `samples <src/samples>`_ folder I'll try to do my best to enrich the documentation in the future (feel to help if you want!!).

The idea is simple:

1) you can define file matching rules as easy as write a regex;
2) on the matching file will applied whatever function you like (abstract from Extractor);
3) store the results in an Elastic index.

Example 1: let's say you want to scan the disk just for jpeg files, extract for each the exif and store it in Elastic.

Example 2: you want to scan all `.exe` or `.dll` files, extract PE header for each of them and store it.

Example 3: you have several distributed machines and you want to centralize information about files in a single location

Take a look here `extractors <src/extractors/README.rst>`_ for further details on extractors.

ToDos
=====

- Add GitHub Action to properly automatically publish the package on PyPi
- Add APScheduler in order to make allow programmable rescan
- Improve (write) documentation
