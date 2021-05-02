<h1>Machine Learning với thư viện MLlib của PySpark</h1>

<h2>Apache Spark</h2>

![Image1](/images/MLlib_1.png)

<h4>Giới thiệu sơ lược</h4>

<p>Apache Spark là một trong những framework được sử dụng rộng rãi trong các dự án xử lí dữ liệu lớn do khả năng xử lý đồ thị, ma trận với tốc độ rất nhanh cùng với việc rất dễ sử dụng làm cho framework này rất phổ biến và được mang vào các giáo trình dạy của các trường đại học chuyên ngành khoa học máy tính</p>
<p>Apache Spark là một framework mã nguồn mở tính toán cụm, được phát triển sơ khởi vào năm 2009 bởi AMPLab. Sau này, Spark đã được trao cho Apache Software Foundation vào năm 2013 và được phát triển cho đến nay</p>
<p>Tốc độ xử lý của Spark có được do việc tính toán được thực hiện cùng lúc trên nhiều máy khác nhau. Đồng thời việc tính toán được thực hiện ở bộ nhớ trong (in-memories) hay thực hiện hoàn toàn trên RAM</p>


<h4>Machine Learning với PySpark</h4>

<p>Nếu bạn đam mê nghiên cứu học máy nhưng với tốc độ xử lý nhanh hơn và với lượng dữ liệu lớn hơn thì đừng lo Apache Spark có thư viện hỗ trợ cho bạn</p>
<p>Thư viện mà Apache Spark cung cấp hỗ trợ nghiên cứu học máy đó là MLlib. Đây là một thư viện vô cùng khổng lồ với rất nhiều tính năng và cũng rất dễ sử dụng. Bạn có thể dùng nó để tạo bất cứ mô hình học máy nào như mô hình hồi qui, mô hình phân loại nhãn,...</p>
<p>Nhưng để sử dụng thật sự hiệu quả và tối đa sức mạnh, tốc độ của framework Spark ta sẽ cần tìm hiểu thêm thư viện Dataframe và SQL của Spark</p>

<h2>Dataframe</h2>



<h3>DataFrame là gì</h3>

<p>DataFrame là một kiểu dữ liệu collection phân tán, được tổ chức thành các cột được đặt tên. Về mặt khái niệm, nó tương đương với các bảng quan hệ (relational tables) đi kèm với các kỹ thuật tối ưu tính toán.</p>
<p>DataFrame có thể được xây dựng từ nhiều nguồn dữ liệu khác nhau như Hive table, các file dữ liệu có cấu trúc hay bán cấu trúc (csv, json), các hệ cơ sở dữ liệu phổ biến (MySQL, MongoDB, Cassandra), hoặc RDDs hiện hành. API này được thiết kế cho các ứng dụng Big Data và Data Science hiện đại. Kiểu dữ liệu này được lấy cảm hứng từ DataFrame trong Lập trình R và Pandas trong Python hứa hẹn mang lại hiệu suất tính toán cao hơn.</p>

![Image1](/images/SQL_Dataframe_1.png)

<h3>Tính năng của DataFrame</h3>

<p>Một số tính năng đặc trưng của DataFrame như:</p>

<ul>
<li>Tối ưu hóa đầu vào: DataFrames sử dụng các công cụ tối ưu hóa đầu vào như Catalyst Optimizer cho phép xử lý dữ liệu hiệu quả.  Ta có thể sử dụng cùng một công cụ cho tất cả các API Python, Java, Scala và R DataFrame.</li>
<li>Xử lý lớn: DataFrames có thể tích hợp với nhiều công cụ BigData khác và cho phép xử lý megabyte đến petabyte dữ liệu cùng một lúc.</li>
<li>Tính linh hoạt: DataFrames, giống như RDD, có thể hỗ trợ nhiều định dạng dữ liệu khác nhau, chẳng hạn như CSV, Cassandra, v.v.</li>
<li>Quản lý bộ nhớ tùy chỉnh: Trong RDD, dữ liệu được lưu trữ trong bộ nhớ RAM, trong khi DataFrames lưu trữ dữ liệu off-heap (bên ngoài không gian chính của Java Heap, nhưng vẫn bên trong RAM), do đó làm giảm các collection quá tải dư thừa.</li>
<li>Xử lý dữ liệu có cấu trúc: DataFrames cung cấp một cái nhìn sơ lược về dữ liệu.  Ở đây, dữ liệu có một số ý nghĩa đối với nó khi nó được lưu trữ</li>
</ul>

<h3>SQL Context</h3>

<p>SQLContext là một lớp và được sử dụng để khởi tạo các chức năng của Spark SQL.  Đối tượng SparkContext là bắt buộc để có thể khởi tạo đối tượng SQLContext.  Lệnh sau được sử dụng để khởi tạo SparkContext thông qua spark-shell.</p>

<h3>Tương tác với Spark DataFrame</h3>
<h4>Config context</h4>

```python
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# Thiết lập Spark context
conf = SparkConf().setMaster("local").setAppName("word counting")
sc = SparkContext.getOrCreate(conf=conf)

# Thiết lập SQL context
sqlContext = SQLContext(sc)
```

<h4>Đọc file csv</h4>
<p>Để đọc file, pyspark.shell cung cấp phương thức read.csv() cho phép đọc file csv vào dataframe.</p>

```python
from pyspark.shell import spark
from pyspark.sql.types import *

# Tạo DataFrame từ file CSV
df_data = spark.read.csv('drive/My Drive/Colab Notebooks/click_data_sample.csv')
print(df_data.head(5))
```

<p>Output:</p>

```python
[Row(_c0='click.at', _c1='user.id', _c2='campaign.id'),
 Row(_c0='2015-04-27 20:40:40', _c1='144012', _c2='Campaign077'),
 Row(_c0='2015-04-27 00:27:55', _c1='24485', _c2='Campaign063'),
 Row(_c0='2015-04-27 00:28:13', _c1='24485', _c2='Campaign063'),
 Row(_c0='2015-04-27 00:33:42', _c1='24485', _c2='Campaign038')]
 ```
<h4>Đổi tên cột</h4>
<p>Ta có thể dễ dàng thay đổi tên cột bằng withColumnRenamed. Tuy nhiên, về cơ bản thì DataFrame là bất biến (immutable) nên khi thay đổi thì 1 DataFrame mới sẽ được tạo ra.</p>

```python
new_df = df_data.withColumnRenamed("_c0", "access_time")\
                .withColumnRenamed("_c1", "userID")\
                .withColumnRenamed("_c2", "campaignID")
print(new_df.printSchema())
```

<p>Output:</p>

```
root
 |-- access_time: string (nullable = true)
 |-- userID: string (nullable = true)
 |-- campaignID: string (nullable = true)

None
```

<h4>Query bằng SQL</h4>
<p>Bằng cách sử dụng registerTempTable, ta sẽ có một table được tham chiếu đến Dataframe đó, ta có thể sử dụng tên table này để viết query SQL. Nếu ta sử dụng sqlContext.sql('query SQL') thì giá trị trả về cũng là Dataframe.</p>
<p>Có 1 lưu ý là: Ta cũng có thể viết subquery nhưng subquery cần được gán Alias, nếu không sẽ bị (Syntax error).</p>
<p>Ta thử tìm các dòng có cột campaignID có giá trị là Campaign047 </p>

```python
# SQL query

new_df.registerTempTable("whole_log_table")

# Query
print(sqlContext.sql(" SELECT * FROM whole_log_table where campaignID == 'Campaign047' ").count())
```

<p>Output:</p>

```python
18081
```
<p>Ta in thử 5 dòng đầu trong đó</p>

```python
print(sqlContext.sql(" SELECT * FROM whole_log_table where campaignID == 'Campaign047' ").show(5))
```

<p>Output:</p>

```python
+-------------------+------+-----------+
|        access_time|userID| campaignID|
+-------------------+------+-----------+
|2015-04-27 05:26:14| 14151|Campaign047|
|2015-04-27 05:26:32| 14151|Campaign047|
|2015-04-27 05:26:34| 14151|Campaign047|
|2015-04-27 05:27:47| 14151|Campaign047|
|2015-04-27 05:28:16| 14151|Campaign047|
+-------------------+------+-----------+
only showing top 5 rows
```

<p>Ta cũng có thể query linh động hơn</p>

```python
#Thêm biến số vào trong câu SQL
for count in range(1, 3):
    print("Campaign00" + str(count))
    print(sqlContext.sql("SELECT count(*) as access_num FROM whole_log_table where campaignID == 'Campaign00" + str(count) + "'").show())
```

<p>Output:</p>

```python
Campaign001
+----------+
|access_num|
+----------+
|      2407|
+----------+

None
Campaign002
+----------+
|access_num|
+----------+
|      1674|
+----------+
none
```

<p>Đối với trường hợp subquery</p>

```python
#Trường hợp Sub Query：
print (sqlContext.sql("SELECT count(*) as first_count FROM (SELECT userID, min(access_time) as first_access_date FROM whole_log_table GROUP BY userID) subquery_alias WHERE first_access_date < '2015-04-28'").show(5))
Output:
+-----------+
|first_count|
+-----------+
|      20480|
+-----------+

None
```

<h4>Tìm kiếm sử dụng filter, select</h4>

<p>Đối với DataFrame , tìm kiếm kèm điều kiện rất đơn giản. Giống với câu query ở trên nhưng filter, select dễ dàng hơn rất nhiều. Vậy filter và select khác nhau thế nào ?</p>
<p>Cùng là để tìm kiếm nhưng filter trả về những row thoả mãn điều kiện, trong đó select  lấy dữ liệu theo column.</p>
<p>Ví dụ Filer</p>

```python
#Ví dụ filter
print(new_df.filter(new_df["access_time"] > "2015-05-01").show(3))
```

```python
Output:

+-------------------+-------+-----------+
|        access_time| userID| campaignID|
+-------------------+-------+-----------+
|           click.at|user.id|campaign.id|
|2015-05-01 22:11:57| 114157|Campaign002|
|2015-05-01 23:36:25|  93708|Campaign055|
+-------------------+-------+-----------+
only showing top 3 rows

None
```

<p>Ví dụ với select</p>

```python
#Ví dụ select
print(whole_log_df.select("access_time", "userID").show(3))
Output:
+-------------------+-------+
|        access_time| userID|
+-------------------+-------+
|           click.at|user.id|
|2015-04-27 20:40:40| 144012|
|2015-04-27 00:27:55|  24485|
+-------------------+-------+
only showing top 3 rows

None
```




<h3>Machine Learning</h3>
<p>Sau khi tìm hiểu qua SQL và Dataframe ta sẽ bắt đầu bước vào phần học máy bằng cách sử dụng thư viện MLlib của Spark</p>

<h4>Giới thiệu sơ lược</h4>

![Image2](/images/MLlib_2.png)

<p>Spark MLlib, viết tát của Machine Learning Library, là một mô-đun nằm trên Spark Core cung cấp các nguyên bản về máy học dưới dạng API. Học máy thường xử lý một lượng lớn dữ liệu để đào tạo mô hình</p>
<p>Khung máy tính cơ sở từ Spark là một lợi ích to lớn. Trên hết, MLlib cung cấp hầu hết các thuật toán thống kê và học máy phổ biến. Điều này giúp đơn giản hóa đáng kể nhiệm vụ làm việc trên một dự án máy học quy mô lớn</p>
<p>Spark MLlib được sử dụng để thực hiện học máy trong Apache Spark. MLlib bao gồm các thuật toán và tiện ích phổ biến. MLlib trong Spark là một thư viện mở rộng của học máy để thảo luận về các thuật toán chất lượng cao và tốc độ cao</p>

<h4>Các công cụ chính mà MLlib cung cấp</h4>

<ul>
  <li>Thuật toán ML: Chúng bao gồm các thuật toán học tập phổ biến như phân loại, hồi quy, phân cụm và lọc cộng tác. MLlib chuẩn hóa các API để giúp kết hợp nhiều thuật toán vào một đường dẫn hoặc quy trình làm việc dễ dàng hơn. Các khái niệm chính là API đường ống, trong đó khái niệm đường ống được lấy cảm hứng từ dự án scikit-learning</li>  
  <li>Featurization: Featurization bao gồm trích xuất, biến đổi, giảm kích thước và lựa chọn như trích xuất, mở rộng, tái tạo và chỉnh sửa</li>  
  <li>Pipelines: Pipelines giúp kết nối các Estimator và Transformer lại với nhau theo một quy trình của làm việc của ML. Đồng thời nó cũng cung cấp công cụ để đánh giá, xây dựng và điều chỉnh ML pipelines</li>
  <li>Utilities: Các công cụ hỗ trợ xử lí đại số tuyến tính, thống kê và xử lý dữ liệu. Ví dụ mllib.linalg cung cấp các hàm hỗ trợ cho đại số tuyến tính</li>  
</ul>

<h4>Sử dụng MLlib tạo ra các mô hình học máy đơn giản</h4>
<h5>Mô hình học máy hồi qui tuyến tính</h5>

```python
from pyspark.ml.classification import LogisticRegression

# Đọc dataset
df = spark.read.csv("data.csv")

# Tách dataset thành dữ liệu training và test
(training_data, test_data) = df.randomSplit([0.8,0.2])

# Khởi tạo mô hình
lr = LogisticRegression(maxIter=10)

# Huấn luyện mô hình
lrModel = v.fit(training_data)

#Thực hiện dự đoán
lrModel.transform(test_data)
```


<h5>Mô hình học máy SVM</h5>

```python
from pyspark.ml.classification import LinearSVC

# Đọc dataset
df = spark.read.csv("data.csv")

# Tách dataset thành dữ liệu training và test
(training_data, test_data) = df.randomSplit([0.8,0.2])

# Khởi tạo mô hình
lsvc = LinearSVC(maxIter=10, regParam=0.1)

# Huấn luyện mô hình
lsvcModel = lsvc.fit(training_data)

#Thực hiện dự đoán
lsvcModel.transform(test_data)
```

<h4>Demo</h4>

<p>Trong phần này sẽ tạo thử một mô hình demo bằng framework PySpark. Ta sẽ sử dụng dataset là Banking Marketing để dự đoán khách hàng như thế nào thì chiến dịch marketting sẽ thành công kêu gọi khách hàng tham gia đăng kí dịch vụ</p>
<p>Dataset này được lấy trên trang <a href="https://www.kaggle.com/">Kaggle</a> được đăng bởi user <a href="https://www.kaggle.com/rouseguy">Rouseguy</a>. Bạn có thể tải lấy dataset <a href="https://www.kaggle.com/rouseguy/bankbalanced/data">tại đây</a> nếu muốn chạy thử demo

<p>Bắt đầu thực nghiệm hóa mô hình bằng framework PySpark</p>
<p>Công việc đầu tiên chúng ta sẽ khởi tạo <b>SparkSession</b> và dùng nó để đọc dataset</p>

```python
from pyspark.sql import SparkSession,SQLContext

# Khởi tạo SparkSession và đặt tên SparkSession đó là "Bank Marketing"
spark = SparkSession.builder.appName("Banking Marketing").getOrCreate();

# Ta sẽ dùng SparkSession vừa khởi tạo để đọc dataset bằng lệnh SparkSession.read.csv(<tên file>)
# Tham số header để khai báo file dataset có header nếu không khai báo thì header trong dataset sẽ mang vô thành data luôn
# Tham số inferSchema sẽ giúp hàm đọc dataset dự đoán kiểu dữ liệu và đổ vào cho hợp lí để loại trừ các dòng bị lệch hay thiếu số nhưng tốc độ sẽ bị chậm
dataframe = spark.read.csv("drive/MyDrive/Colab Notebooks/bank.csv", header=True, inferSchema=True)
```

<p>Để kiểm tra dataset đọc có thành công không ta dùng <b>dataframe.show()</b> để hiện ra dataset chúng ta trông như thế nào</p>

```python
dataframe.show()

Output:
+---+-----------+--------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+-------+
|age|        job| marital|education|default|balance|housing|loan|contact|day|month|duration|campaign|pdays|previous|poutcome|deposit|
+---+-----------+--------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+-------+
| 59|     admin.| married|secondary|     no|   2343|    yes|  no|unknown|  5|  may|    1042|       1|   -1|       0| unknown|    yes|
| 56|     admin.| married|secondary|     no|     45|     no|  no|unknown|  5|  may|    1467|       1|   -1|       0| unknown|    yes|
| 41| technician| married|secondary|     no|   1270|    yes|  no|unknown|  5|  may|    1389|       1|   -1|       0| unknown|    yes|
| 55|   services| married|secondary|     no|   2476|    yes|  no|unknown|  5|  may|     579|       1|   -1|       0| unknown|    yes|
| 54|     admin.| married| tertiary|     no|    184|     no|  no|unknown|  5|  may|     673|       2|   -1|       0| unknown|    yes|
| 42| management|  single| tertiary|     no|      0|    yes| yes|unknown|  5|  may|     562|       2|   -1|       0| unknown|    yes|
| 56| management| married| tertiary|     no|    830|    yes| yes|unknown|  6|  may|    1201|       1|   -1|       0| unknown|    yes|
| 60|    retired|divorced|secondary|     no|    545|    yes|  no|unknown|  6|  may|    1030|       1|   -1|       0| unknown|    yes|
| 37| technician| married|secondary|     no|      1|    yes|  no|unknown|  6|  may|     608|       1|   -1|       0| unknown|    yes|
| 28|   services|  single|secondary|     no|   5090|    yes|  no|unknown|  6|  may|    1297|       3|   -1|       0| unknown|    yes|
| 38|     admin.|  single|secondary|     no|    100|    yes|  no|unknown|  7|  may|     786|       1|   -1|       0| unknown|    yes|
| 30|blue-collar| married|secondary|     no|    309|    yes|  no|unknown|  7|  may|    1574|       2|   -1|       0| unknown|    yes|
| 29| management| married| tertiary|     no|    199|    yes| yes|unknown|  7|  may|    1689|       4|   -1|       0| unknown|    yes|
| 46|blue-collar|  single| tertiary|     no|    460|    yes|  no|unknown|  7|  may|    1102|       2|   -1|       0| unknown|    yes|
| 31| technician|  single| tertiary|     no|    703|    yes|  no|unknown|  8|  may|     943|       2|   -1|       0| unknown|    yes|
| 35| management|divorced| tertiary|     no|   3837|    yes|  no|unknown|  8|  may|    1084|       1|   -1|       0| unknown|    yes|
| 32|blue-collar|  single|  primary|     no|    611|    yes|  no|unknown|  8|  may|     541|       3|   -1|       0| unknown|    yes|
| 49|   services| married|secondary|     no|     -8|    yes|  no|unknown|  8|  may|    1119|       1|   -1|       0| unknown|    yes|
| 41|     admin.| married|secondary|     no|     55|    yes|  no|unknown|  8|  may|    1120|       2|   -1|       0| unknown|    yes|
| 49|     admin.|divorced|secondary|     no|    168|    yes| yes|unknown|  8|  may|     513|       1|   -1|       0| unknown|    yes|
+---+-----------+--------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+-------+
only showing top 20 rows
```

<p>Tiếp theo ta sẽ thực hiện xử lý dữ liệu thành kiểu dữ liệu thành các con số để có thể train được mô hình</p>
<p>Ta sẽ sử dụng <b>StringIndexer</b> để chuyển kiểu dữ liệu chuỗi sang số</p>

```python
from pyspark.ml.feature import StringIndexer

for i in dataframe.schema:
  indexer = StringIndexer()
  indexer.setInputCol(i).setOutputCol(i+"_indexer")
  df = indexer.fit(df).transform(df)
```

<p>Thông qua <b>Output</b> trên ta thấy cột job, marital, education, default, housing, loan, contact, month, poutcome và deposit là có kiểu dữ liệu
<h1>Tài liệu tham khảo</h1>
<ul>
  <li>[1] https://viblo.asia/p/tim-hieu-ve-apache-spark-ByEZkQQW5Q0</li> 
  <li>[2] https://towardsdatascience.com/machine-learning-at-scale-with-apache-spark-mllib-python-example-b32a9c74c610</li>
  <li>[3] https://spark.apache.org/docs/latest/ml-features.html#vectorassembler</li>
</ul>
