# Search Join

Given a user-provided query table, a Search Join [1] finds related tables and integrates them with the query table.
The result is an enriched query table with additional attributes.

## How to run

To run the search join, you first need to create an index over a corpus of tables:

```bash
#!/bin/bash

JAR="insert path to jar file here"
CLS="de.uni_mannheim.informatik.dws.searchjoin.cli.TableIndexing"
IDX="index/"
WEB="insert path to tables here"

java c- $JAR $CLS -index $WEB
```

Run the search join with the just created index:

```bash
#!/bin/bash

JAR="insert path to jar file here"
CLS="de.uni_mannheim.informatik.dws.searchjoin.cli.SearchJoin"
IDX="index/"
QUERY="insert path to query table(s) here"
RESULT="result/"

java c- $JAR $CLS -index $WEB -out $RESULT $QUERY
```

## Acknowledgements

This project is a simplified implementation of the Mannheim Search Join Engine [1] based on the [WInte.r Framework](https://github.com/olehmberg/winter).
It is designed to be used with the [Web Data Commons Web Tables](http://webdatacommons.org/webtables/) corpora. Other sources of tables can be used as long as they use the same data format.

## License

The Search Join code can be used under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).

## References

[1] Lehmberg, O., Ritze, D., Ristoski, P., Meusel, R., Paulheim, H., & Bizer, C. (2015). The Mannheim Search Join Engine. Web semantics: science, services and agents on the World Wide Web, 35, 159-166.