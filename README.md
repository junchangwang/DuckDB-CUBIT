## give DuckDB a TopDown Optimizer
我们要做的是给原本是基于BottomUp优化器的DuckDB添加一个新的TopDown优化器。
在直接fork了DuckDB的基础上，zhaojy20给main/src/optimizer和main/src/include/optimizer分别添加了cascade目录，在此目录内将包含所有被用于实现TopDown优化器的源码或头文件。
目前准备基于Orca的源码From scratch地完成所有代码的编写。
Orca源码已经复制到了main/Orca中。
