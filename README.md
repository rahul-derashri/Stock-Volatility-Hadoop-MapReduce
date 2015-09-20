# Computing Volatility of Stocks in NASDAQ using Mapreduce
                          

**Method and Implementation**
**Mapper-1**
We have been given 2970 stocks data each having three years of data. The first mapreduce job will
read all the input CSV files with number of mappers equal to the number of files. Each mapper will
read the data line by line. Each line contains 7 data information i.e. Date, Open, High, Low, Close,
Volume and Adj Close. But for the calculation of volatility only Adj Close and date is required, so first
mapper will map set filename (Stock Name) as the key and comma separated values of date and adj
close as the values as explained in Figure 1

**Reducer - 1**
So after mapping process, reducer will get filename as key and all the values associated with the
similar key as iteratable values. Reducer -1 will basically calculate Rate of Return for a stock for each
month and produce output with key as stock name and single value containing the Rate of Return of
a month [Figure 2]. The logic used for taking out the start and end date Adj Close values is that take
out the first values from the iteratable and set that start and initialize the month. Then loop (While
Loop) over the rest of the values and take the previous and current values whenever the month
changes. And at the end take the last value. Rate of Return will be calculated each time the month
changes. The reducer will produce one key value pair for every month of a stock.

**Mapper - 2**
Mapper -2 is not making much changes to the data. It will take key values from the last Reducer -1
output and the same data will be sent to the Reducer – 2.

**Reducer - 2**
Reducer -2 will take the key as the stock name and then iteratable values will have the Rate of Return
for all the months for a stock. It will calculate the Volatility of the stock according to the formula
Volatility = 
and produce the output with key as stock name and values as volatility of the stock.

**Mapper - 3**
Mapper – 3 is used specifically for sorting the data according to the volatility. It will take the same
key value pair given by Reducer – 2 as input and exchange the key with value and value with key so
that it will sort the data according to volatility and give it to the reducer.

**Reducer – 3**
It will generate the final solution. This will have input with volatility as key and stock name as value.
This will be in sorted order of volatility as mapper sends data sorted according to the key to the
reducer. So now reducer will just take the first 10 and last 10 stocks which will give 10 stocks with
lowest volatility and 10 stocks with highest volatility, respectively, as output. Cleanup method is used
to print the top 10 and bottom 10 stocks as it can be done only after completion of reduce task.

Experiment and Discussion
The experiments are conducted on CCR in UB.
The program is first developed on the eclipse IDE in one single machine, then the jar file is exported
and delivered to the CCR and executed.

First I tried with increasing the number of nodes (1, 2 and 4) to do the processing. Every increase has
reduced the time to approximately half of the time for small dataset. As you can see from the table
timings are almost half every time. Then I tried the same process for medium and large dataset which
has exactly followed the same curve i.e. halving every time on increasing of nodes.

