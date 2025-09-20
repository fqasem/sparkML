# Objectives
Project showing how to use Apache Spark for:

- ETL - parquet to CSV, CSV to parquet. Clean Data - drop duplicate, drop nul etc.
- Machine Learning - refression clustering, classification
- Spark SQL - loading data drom database and analyzing data
- Handling streaming data


Set Up Requirements:

1. Download Java
2. Set JAVA_HOME
3. Add %JAVA_HOME%\bin to PATH. Check by typing java -version in command prompt.
4. Download Python and select this as the puthon kernel for you Jupyter notebooks
5. Check the spark and java versions compatibility matrix at https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669
6. Download spark with hadoop from Apache Spark page. Extract it.
7. Set this as your SPARK_HOME
8. Start spark server by running ./bin/pyspark in Spark directory.
9. Guide for Spark set up: https://spark.apache.org/docs/latest/quick-start.html
10. Note: Used JDK 17 with PySpark 4.0.0 (spark-4.0.0-bin-hadoop3 as the spark server)

# Background

## Data Engineering Pipelines
In today's data-driven world, organizations are constantly seeking valuable insights and using advanced algorithms to make informed decisions. Data engineering pipelines are the foundation of successful data-driven projects. They handle the collection, transformation, and storage of large amounts of raw data. Data engineers design and implement robust systems to handle data at scale. They use tools and technologies to clean, transform, and integrate different data sources into a reliable format.

Data quality is essential in data engineering. Engineers create efficient data pipelines to process data smoothly. These pipelines involve extracting data from various sources, making necessary changes, and storing it in storage or analytical systems. Data engineers collaborate with data scientists, analysts, and stakeholders to understand their needs and provide them with clean and accessible data.

Let's take a closer look at the four key parts that make up the data engineering pipelines:

### Data Collection: 
Data engineers are responsible for gathering data from various sources. This includes databases, APIs, web scraping, streaming platforms, and more. By combining data from multiple sources, data engineers ensure a comprehensive and diverse dataset to work with.

### Data Transformation: 
Once the data is collected, it undergoes a series of transformations to ensure its quality and usefulness. This involves preprocessing steps such as cleaning the data to remove errors and inconsistencies, handling missing values, and addressing outliers. Data engineers also apply techniques like data normalization and standardization to ensure uniformity and comparability across different data points.

### Data Integration: 
Organizations often deal with data coming from different sources and in different formats. Data engineers play a crucial role in integrating this unrelated data to create a unified and coherent dataset. This involves merging data from various sources, performing data joins to combine related information, and aggregating data to obtain a consolidated view.

### Data Storage: 
The processed and integrated data needs to be stored in a suitable repository for easy accessibility and scalability. Data engineers utilize data warehousing systems or data lakes to store the data securely and efficiently. These repositories provide the foundation for further analysis and serve as a centralized hub for data-driven operations.



Data engineering pipelines are designed to handle large volumes of data efficiently. They ensure the smooth flow of data from collection to storage, laying the groundwork for subsequent analysis and insights.

## Machine Learning Pipelines
Machine learning pipelines play a crucial role in extracting valuable insights from data using algorithms. These pipelines cover the entire process of creating, training, and deploying machine learning models.

Data scientists and machine learning engineers collaborate closely to optimize the pipelines and improve model performance. Techniques such as data sampling, feature extraction, and model selection ensure accurate predictions and meaningful outputs. Additionally, concepts like cross-validation, hyperparameter tuning, and effective model deployment strategies are incorporated into machine learning pipelines to create robust and scalable solutions.

Let's explore the five essential parts that make up these pipelines:

### Data Preprocessing: 
Raw data often requires preprocessing before it can be used effectively for machine learning tasks. Data preprocessing involves handling missing values, dealing with categorical variables by encoding or one-hot encoding them, and normalizing numerical features to bring them within a consistent range. This step ensures that the data is in a suitable format for the subsequent stages of the pipeline.

### Feature Engineering: 
Feature engineering is the process of selecting, creating, or transforming features in the dataset to enhance the performance of machine learning models. This involves extracting relevant features, creating new features based on domain knowledge, or applying techniques like dimensionality reduction to reduce the complexity of the data. Feature engineering plays a crucial role in capturing the underlying patterns and relationships in the data.

### Model Training: 
In this stage, machine learning algorithms are applied to the preprocessed and engineered dataset to learn patterns and make predictions. Data scientists and machine learning engineers select the appropriate algorithms based on the nature of the problem and the available data. The models are trained using labeled data, allowing them to learn from the patterns and make accurate predictions or classifications.

### Model Evaluation: 
Trained models need to be evaluated to assess their performance and generalizability. Various metrics, such as accuracy, mean squared error(MSE), precision, recall, and F1 score, are used to evaluate the models' predictive capabilities. Cross-validation techniques are employed to validate the models' performance on unseen data, ensuring that they can generalize well beyond the training data.

### Model Deployment: 
The best-performing model is deployed into a production environment, where it can make predictions or generate insights on new and unseen data. This stage involves integrating the model into existing systems or creating APIs that allow for easy integration with other applications. Model deployment strategies ensure that the models are scalable, robust, and capable of handling real-time data streams.

Machine learning pipelines empower organizations to leverage algorithms effectively and make data-driven decisions, enabling automation, optimization, and predictive capabilities.

## Bridging the Gap: Data Engineering Pipelines and Machine Learning Pipelines
Data engineering pipelines and machine learning pipelines may have different focuses, but they are closely connected and support each other. Data engineering pipelines provide clean and well-structured data, which is essential for successful machine learning pipelines. Without good data, machine learning models cannot make accurate predictions or find meaningful insights.

Data engineering pipelines feed the necessary inputs to machine learning pipelines for model training and evaluation. In return, machine learning pipelines provide valuable feedback to improve the data engineering process. The insights and predictions from machine learning models help identify areas where data engineering can be enhanced. This continuous feedback loop ensures that data engineering pipelines adapt to the evolving needs of machine learning workflows.

## Real-world Applications
The collaboration between data engineering pipelines and machine learning pipelines enables organizations to achieve various goals. Here are some common scenarios:

### Real-time Decision Making: 
Data engineering pipelines process and deliver data in real-time, allowing machine learning models to make timely and informed decisions. This is crucial for applications like fraud detection, recommendation systems, and dynamic pricing. Data engineers ensure that data is collected and processed promptly, providing inputs for real-time machine learning predictions.

### Scalable Data Processing: 
Data engineering pipelines are designed to efficiently handle large amounts of data, ensuring scalability and performance. Machine learning pipelines can take advantage of this capability to process extensive datasets and train complex models. By optimizing data processing and storage, data engineers enable machine learning pipelines to handle large-scale data analysis and generate valuable insights.

### Model Deployment and Monitoring: 
Data engineering pipelines facilitate the seamless deployment of machine learning models in production environments. They ensure data consistency, security, and maintainability, while machine learning pipelines enable model monitoring and performance optimization. Data engineers work alongside machine learning engineers to ensure effective deployment and integration of models, along with robust and scalable data pipelines.

# Conclusion
Data engineering pipelines and machine learning pipelines play vital roles in the data-driven landscape. Data engineering establishes a reliable data infrastructure, while machine learning pipelines leverage algorithms to extract valuable insights. Through collaboration and integration, these pipelines transform raw data into actionable intelligence, empowering organizations to make informed decisions and stay competitive in today's data-driven world. The synergy between data engineering and machine learning pipelines unlocks the true value of data and drives innovation across various domains.
