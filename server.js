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

    const query = `SELECT DISTINCT c.id, 
        c.company_name, 
        p.project_name, 
        p.budget_totals_actual_project_detail_gross_profit_amount_total AS gross_profit, 
        cp.contract_price__v_double AS contract_price, 
        ROUND(p.budget_totals_actual_project_detail_owner_variation_total / 100.0) * 100 AS variation, 
        p.budget_totals_actual_project_detail_owner_price_total AS total_owner_price, 
        ROUND(p.budget_totals_actual_project_detail_owner_variation_total / 100.0) * 100 + p.project_detail_invoiced_created AS invoiced, 
        p.project_detail_invoiced_total AS payment_received, 
        p.project_detail_payment_due AS payment_due, 
        p.budget_totals_actual_project_detail_actual_total_total AS actual, 
        p.budget_totals_project_detail_builder_variation_total AS builder_variation_total, 
        p.budget_totals_actual_project_detail_builder_cost_total AS builder_cost_total,
        DATE(TIMESTAMP_MILLIS(p.updated_at)) AS date
    FROM build-task-staging.firestore_company.company_raw_changelog AS c 
    LEFT JOIN build-task-staging.firestore_project.projects_raw_changelog AS p ON c.id = p.company_id 
    LEFT JOIN build-task-staging.firestore_contract_payments.contract_payments_raw_changelog AS cp ON c.id = cp.company_id 
    WHERE c.id = 'FPuPgQizTPbibAOeDKYx'`

    const [job] = await bigquery.createQueryJob({ query })
    console.log('[CONNECTION LOG] Data successfully fetched...')

    const [rows] = await job.getQueryResults()
    return rows
}

// Define an API endpoint for the BigQuery query
app.get('/api/query', async (req, res) => {
    try {
        const data = await queryBigQuery()
        res.json(data)
    } catch (error) {
        console.error('[ERROR LOG]', error)
        res.status(500).json({ error: 'Failed to fetch data from BigQuery' })
    }
})

const server = http.createServer(app)
const port = process.env.PORT || 3000

server.listen(port, () => {
    console.log('[SERVER LOG] Server running on port', port)
})
