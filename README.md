# The Apriori Algorithm: Big Data Mining in Market Basket Analysis
---

## Market Basket Analysis with Hadoop MapReduce

This repository contains the implementation of **Market Basket Analysis (MBA)** using the Apriori algorithm on the Hadoop ecosystem. The project demonstrates how to process large-scale transactional data using MapReduce and Pig Latin scripts for generating insights into customer purchasing behavior.

---

### **Project Overview**
Market Basket Analysis is a popular data mining technique used to identify relationships between items in a dataset. It is widely applied in retail, e-commerce, and other industries to optimize operations such as cross-selling, inventory management, and pricing strategies.

The Apriori algorithm identifies frequent itemsets and derives association rules, providing actionable insights. This project showcases its implementation using Hadoop MapReduce and Pig scripting for scalable data processing.

---

### **Directory Structure**
The repository is organized as follows:

```
├── Dataset/                # Folder containing the transactional datasets used for analysis
├── Report & Output/        # Folder with generated reports and final outputs
├── avg_basket.pig          # Pig script for calculating the average basket size
├── country_wise.pig        # Pig script for analyzing transactions by country
├── part1.java              # MapReduce job for finding frequent 1-itemsets
├── part2.java              # MapReduce job for generating candidate k-itemsets
├── part3.java              # MapReduce job for filtering frequent k-itemsets
├── popitems.pig            # Pig script for identifying the most popular items
├── revenuepercountry.pig   # Pig script for calculating revenue per country
```

---

### **Implementation Details**

#### **Hadoop MapReduce (Java Implementation)**
1. **`part1.java`**:  
   - Identifies frequent 1-itemsets from the dataset.  
   - Outputs itemsets with their respective support counts.  

2. **`part2.java`**:  
   - Generates candidate k-itemsets using the Apriori algorithm.  
   - Performs pruning based on the support threshold.  

3. **`part3.java`**:  
   - Filters frequent k-itemsets by comparing support counts with the threshold.  
   - Outputs the final frequent itemsets.

#### **Pig Scripting**
1. **`avg_basket.pig`**:  
   - Calculates the average size of customer transactions (basket size).

2. **`country_wise.pig`**:  
   - Analyzes transaction patterns across different countries.

3. **`popitems.pig`**:  
   - Identifies the most popular items purchased by customers.

4. **`revenuepercountry.pig`**:  
   - Computes revenue generated by each country.

---

### **Steps to Run the Project**

#### **Prerequisites**
- Hadoop and HDFS installed and configured.
- Apache Pig installed.
- Java Development Kit (JDK) installed.

#### **Running the MapReduce Jobs**
1. Compile the Java programs:
   ```bash
   javac -classpath `hadoop classpath` -d . part1.java part2.java part3.java
   jar -cvf MBA.jar *.class
   ```
2. Upload the dataset to HDFS:
   ```bash
   hdfs dfs -put /local/path/to/dataset /hdfs/path/to/dataset
   ```
3. Run the MapReduce jobs in sequence:
   ```bash
   hadoop jar MBA.jar part1 /hdfs/path/to/dataset /hdfs/path/output1
   hadoop jar MBA.jar part2 /hdfs/path/output1 /hdfs/path/output2
   hadoop jar MBA.jar part3 /hdfs/path/output2 /hdfs/path/final_output
   ```

#### **Running Pig Scripts**
1. Execute Pig scripts:
   ```bash
   pig avg_basket.pig
   pig country_wise.pig
   pig popitems.pig
   pig revenuepercountry.pig
   ```

---

### **Outputs**
- **Frequent Itemsets**: Stored in the `/hdfs/path/final_output` directory.
- **Analytical Insights**:  
   - Average basket size  
   - Popular items  
   - Country-wise transaction trends  
   - Revenue by country  

---

### **Use Cases**
- **Retail**: Optimize store layouts, inventory management, and product placement.  
- **E-commerce**: Enable cross-selling, upselling, and recommendation engines.  
- **Marketing**: Tailor promotions and loyalty programs based on purchasing behavior.

---

### **Contributing**
Feel free to raise issues or submit pull requests for improvements. Contributions are always welcome!

---

### **License**
This project is licensed under the MIT License. See the LICENSE file for details.

--- 

