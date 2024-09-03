require('dotenv').config()

const http = require('http')
const express = require('express')
const cors = require('cors')
const { BigQuery } = require('@google-cloud/bigquery')

const app = express()
app.use(cors())
app.use(express.urlencoded({ extended: true }))
app.use(express.json())

const bigquery = new BigQuery()

const queryBigQuery = async () => {
    console.log('[CONNECTION LOG] Querying data from BigQuery...')

    const query = "select distinct c.id, c.company_name, p.project_name, p.budget_totals_actual_project_detail_gross_profit_amount_total as gross_profit, cp.contract_price__v_double as contacrt_price, ROUND(p.budget_totals_actual_project_detail_owner_variation_total / 100.0) * 100 AS variation, p.budget_totals_actual_project_detail_owner_price_total as Total_Owner_price, ROUND(p.budget_totals_actual_project_detail_owner_variation_total / 100.0) * 100 + p.project_detail_invoiced_created as invoiced, p.project_detail_invoiced_total as payment_recieved, p.project_detail_payment_due as payment_due, p.budget_totals_actual_project_detail_actual_total_total AS actual, p.budget_totals_project_detail_builder_variation_total as builder_varation_total, p.budget_totals_actual_project_detail_builder_cost_total as builder_cost_total, from build-task-staging.firestore_company.company_raw_changelog as c left join build-task-staging.firestore_project.projects_raw_changelog as p on c.id = p.company_id left join build-task-staging.firestore_contract_payments.contract_payments_raw_changelog as cp on c.id = cp.company_id where p.id = '5Pgw89EnNj6kJhcHQHDi'"

    const [job] = await bigquery.createQueryJob({ query })
    console.log('[CONNECTION LOG] Data successfully fetched...')

    const [rows] = await job.getQueryResults()
    rows.forEach((row) => console.log(row))
}

const server = http.createServer(app)
const port = process.env.PORT

server.listen(port, () => {
    console.log('[SERVER LOG] Server running on port', port)
    queryBigQuery()
})