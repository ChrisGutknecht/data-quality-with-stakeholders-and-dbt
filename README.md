## Welcome to my Session for "Building data quality with stakeholders"

<img width="250" alt="noslides" src="https://noslides.wtf/_app/immutable/assets/hero-desktop.DMAxQ2GM.webp">

---
### 1. INTRO. Data quality is like a campfire :fire:

---
* We want people to stay and spend time on data quality... but HOW?

<img width="550" alt="campfire" src="https://magazine.outdoornebraska.gov/wp-content/uploads/2023/07/EF20090804_049-cmyk-copy.jpg">


* How do people gather round a campfire? **Marshmallows**!

<img width="550" alt="campfire-with-marshmallows" src="https://img.freepik.com/premium-photo/cozy-mug-with-marshmallows-by-campfire-with-friends_464863-3401.jpg">

* **Marshmallows**: 
  * Data events that stakeholders really care about:
  * Example. **Anomalies** on leading indicators, which they don't have to monitor
* BUT we need to mix in **healthy snacks** = boring tests like `unique` and `not_null`

---


## 2. POLL. Let's have a Live poll to get know you!

Link: https://www.menti.com/alxmw7icd8ks
Code: 4553 5553

<img width="250" alt="poll" src="https://vscteam.de/wp-content/uploads/2021/03/Mentimeter-Logo.png">


---

## 3. DEMO. Let's dive into some real examples!


### Types of heavily used data tests


| #   | Test type         | Example use cases       | Stakeholder interaction  |
|-----|-------------------|-------------------------|--------------------------|
| 1   | Live alerts       | 404 errors              | High                     |
| 2   | Volume changes    | Googlebot crawl volume  | Medium                   |
| 3   | Date completeness | Missing dates           | Low                      |
| 4   | dbt generic tests | not_null, unique        | None                     |

---
### Live alerts demo: 
See recording: https://www.linkedin.com/events/buildingdataqualitywithyourstak7243873241482579968/comments/

---
### Our daily data quality alert pipeline @ Bergzeit :mountain_snow:

* We use **dbt cloud** to manage models and run tests
* We store the test failures with an `on-run-end` macro `store_test_results` in BigQuery
* Listen on table updates with **Logs explorer** sink and PubSub and trigger a cloud function
* A `post_dbt_test_result` cloud function queries the `test_results` dbt model, converts the dataframe to HTML and sends it to MS teams

This is how ChatGPT visualizes the process: :-D

<img width="350" alt="poll" src="cloud_components_chatgpt.png">

---

### How we resolve data tests with our stakeholders

* Use dbt Explore to get the compiled code of the failed test and run the query
* For stakeholders, provide additional Looker Studio Dashboards for Data Visualization
* Rotate first response duty for test failure among data team
 
  * First time the failure? Threshold too strict? Deduplication via a qualify row_number()?
  * improve signal-to-noise ratio: Is the alert meaningful? Is there an adequate business response, or nothing to be done?
  * keep improving  model description to 1) provide sufficient context, 2) describe worst case Scenarios und 3) specific resolution steps

---

### Best practices for improving data tests

* Route different tests into differente channels (Teams, Slack) using tags, either on model and test level

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

* use **owner** tags on all models

```
  - name: stg_gsc_inspection_logs
    description: >
      This model lists the search console inspection logs for a list daily tested URLs. 
      See source description for more details
    meta:
      owner: "@Chris G"
```
* use group tags for all folders as **fallback** for model ownership

```
census_syncs:
  +group: data_team

channel_attribution:
  +group: customer_acquisition
```
* How can I optimize the UX test alerts resolution?

# Resources: 
- dbt expectations: https://github.com/calogica/dbt-expectations


