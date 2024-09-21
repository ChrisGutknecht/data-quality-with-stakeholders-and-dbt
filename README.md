## Welcome to my No-Slides session for "Data quality with stakeholders"

### 1. INTRO. Data quality is like a campfire (Inspiration: Chad Sanderson)

---
* We want people to stay and spend time on data quality... but how?

(remove before publish: Data team level
* Beginners. "Where do we look for material?"
* Intermediates. "Something's always on fire! This material is sh...! "
* Experts. "Everything's under control. We have friends helping us")
 
<img width="550" alt="campfire" src="https://magazine.outdoornebraska.gov/wp-content/uploads/2023/07/EF20090804_049-cmyk-copy.jpg">

* How do people gather round the fire? Marshmallows!

<img width="550" alt="campfire-with-marshmallows" src="https://img.freepik.com/premium-photo/cozy-mug-with-marshmallows-by-campfire-with-friends_464863-3401.jpg">

* **Marshmallows** = data events stakeholders care about)
* But... we need to mix snacks with **healthy food**!
* Healthy food = Boring tests like `unique` and `not_null`

---

## 2. POLL. Let's have a Live poll to get know YOU!

Link: https://www.menti.com/alxmw7icd8ks
Code: 4553 5553

<img width="350" alt="poll" src="https://vscteam.de/wp-content/uploads/2021/03/Mentimeter-Logo.png">


---

## 3. DEMO. Let's dive into some real examples!

### Our data quality alert pipeline 
* **dbt cloud** to manage models and run tests
* Store the test failures with an `on-run-end` macro `store_test_results` in BigQuery
* Listen on table updates with **Logs explorer** sink and PubSub and trigger a cloud function
* A `post_dbt_test_result` cloud function queries the `test_results` dbt model, converts the dataframe to HTML and sends it to MS teams

This is how ChatGPT visualizes the process: 

<img width="350" alt="poll" src="https://gist.github.com/user-attachments/assets/196817d4-cce7-4147-a373-c216fa86cda6">

---

### Types of heavily uses data tests


| #   | Test type         | Example use cases       | Stakeholder interaction |
|-----|-------------------|-------------------------|--------------------------|
| 1   | live alerts       | 404 errors              | High                     |
| 2   | volume changes    | Googlebot crawl volume  | Medium                   |
| 3   | date completeness | Missing dates           | Low                      |
| 4   | dbt generic tests | not_null, unique        | None                     |

---

### How we resolve data tests with our stakeholders


* Use dbt Explore to demonstrate how to debug data tests
* run the compiled query in BigQuery to find out the source
* Looker Studio Dashboards for Data Visualization

---

### Best practices for improving data tests

* Route different tests into differente channels (Teams, Slack) using

```
  - dbt_expectations.expect_table_row_count_to_be_between:
      # The three cloud schedulder jobs for DACH query 350 URLs each, thus a total min amount is expected
      min_value: 900
      row_condition: "date = current_date()" # (Optional)
      strictly: false
      config:
        severity: warn
        tags: ["analytics-alerts"]
```

* use owner tags on all models

```
  - name: stg_gsc_inspection_logs
    description: >
      This model lists the search console inspection logs for a list daily tested URLs. 
      See source description for more details
    meta:
      owner: "@Chris G"
```
* use group tags for all folders as fallback for model ownership

```
census_syncs:
  +group: data_team

channel_attribution:
  +group: customer_acquisition
```

* constantly update the model description to give context, describe worst case Scenarios of test failues und specific resolution tests

