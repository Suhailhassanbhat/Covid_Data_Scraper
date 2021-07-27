# Ky. COVID-19 Scraper

This scraper runs every half hour and gets all the data published by Kentucky Department of Health https://govstatus.egov.com/kycovid19

Camelot libraries do most of the work since the dept. publishes daily reports in pdf format. BeautifulSoup gets links to new reports, pandas, numpy, regex for cleaning the data and putting it in tabular form.

Template by Jonathan Soma @dangerscarf data head, python kid, co-founder @bkbrains, director @ledeprog


Using GitHub Actions and Python, this repo automatically scrapes the content of the BBC's homepage every 10 minutes and saves it [right into the repo itself](https://github.com/jsoma/autoscraper-bbc/blob/main/bbc-headlines.csv). 

Just a simple Python-driven example of @simonw's [Git Scraping](https://simonwillison.net/2020/Oct/9/git-scraping/). See also See also [autoscraper-mailer](https://github.com/jsoma/autoscraper-mailer), which sends mail, and [autoscraper-changes](https://github.com/jsoma/autoscraper-changes), which only mails if there have been changes.
