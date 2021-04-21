# goclassy

<img align="left" src="img/logo.png" width="200" height="200" /> 

This is the **old** [OSCAR](https://oscar-corpus.com) pipeline, if you are looking for the upcoming pipeline please take a look at the [Ungoliant](https://github.com/oscar-corpus/ungoliant) project.

An asynchronous concurrent pipeline for classifying [Common Crawl](http://commoncrawl.org/) based on [fastText's pipeline](https://github.com/facebookresearch/fastText/tree/master/crawl).

For more info see our paper [here](http://corpora.ids-mannheim.de/CMLC7-final/CMLC-7_2019-Oritz_et_al.pdf).

If you want to download OSCAR you can do it [here](https://oscar-corpus.com).

**Note:** For the moment the downloader part of the pipeline is not available as it is still experimental, it will be open sourced in a future release.

## References

```text
@inproceedings{OrtizSuarezSagotRomary2019,
  author    = {Pedro Javier {Ortiz Su{\'a}rez} and Beno{\^i}t Sagot and Laurent Romary},
  title     = {Asynchronous pipelines for processing huge corpora on medium to low resource infrastructures},
  series = {Proceedings of the Workshop on Challenges in the Management of Large Corpora (CMLC-7) 2019. Cardiff, 22nd July 2019},
  editor    = {Piotr Bański and Adrien Barbaresi and Hanno Biber and Evelyn Breiteneder and Simon Clematide and Marc Kupietz and Harald L{\"u}ngen and Caroline Iliadi},
  publisher = {Leibniz-Institut f{\"u}r Deutsche Sprache},
  address   = {Mannheim},
  doi       = {10.14618/ids-pub-9021},
  url       = {http://nbn-resolving.de/urn:nbn:de:bsz:mh39-90215},
  pages     = {9 -- 16},
  year      = {2019},
  abstract  = {Common Crawl is a considerably large, heterogeneous multilingual corpus comprised of crawled documents from the internet, surpassing 20TB of data and distributed as a set of more than 50 thousand plain text files where each contains many documents written in a wide variety of languages. Even though each document has a metadata block associated to it, this data lacks any information about the language in which each document is written, making it extremely difficult to use Common Crawl for monolingual applications. We propose a general, highly parallel, multithreaded pipeline to clean and classify Common Crawl by language; we specifically design it so that it runs efficiently on medium to low resource infrastructures where I/O speeds are the main constraint. We develop the pipeline so that it can be easily reapplied to any kind of heterogeneous corpus and so that it can be parameterised to a wide range of infrastructures. We also distribute a 6.3TB version of Common Crawl, filtered, classified by language, shuffled at line level in order to avoid copyright issues, and ready to be used for NLP applications.},
  language  = {en}
}
```
