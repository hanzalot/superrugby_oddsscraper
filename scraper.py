import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime
import scraperwiki

class OddscheckerSpider(scrapy.Spider):
    name = "ocs"
    
    #allowed_domains = ["http://www.oddschecker.com/"]
    start_urls = [
        "http://www.oddschecker.com/rugby-union/super-rugby"
    ]

    def parse(self, response):
        
        games = response.xpath('//*[@id="fixtures"]/div/table/tbody/tr/td[5]/a')
        for game in games:
            link = game.xpath('.//@href').extract()
            url = response.urljoin(link[0])
            request = scrapy.Request(url, callback=self.parseMatch)
            yield request
            
    def parseMatch(self, response):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        matchname = response.css('#betting-odds > section:nth-child(1) > div > header > h1::text').extract()[0]
        
        # Create list of bookies
        bookies = response.xpath('//*[@id="oddsTableContainer"]/table/thead/tr[4]/td')
        winner_1_row = response.xpath('//*[@id="t1"]/tr[1]') #//*[@id="t1"]/tr[1]/td[3]
        winner_2_row = response.xpath('//*[@id="t1"]/tr[2]') #//*[@id="t1"]/tr[2]/td[8]
        draw_row = response.xpath('//*[@id="t1"]/tr[3]')
        winner_1 = response.xpath('//*[@id="t1"]/tr[1]/@data-bname').extract()[0]
        winner_2 = response.xpath('//*[@id="t1"]/tr[2]/@data-bname').extract()[0]
        winner_1_odds = response.css('#t1 > tr:nth-child(1) > td')
        winner_2_odds = response.css('#t1 > tr:nth-child(2) > td')
        draw_odds = response.css('#t1 > tr:nth-child(3) > td')
        
        print winner_1 + ' vs ' + winner_2
        
        num_bookies = len(bookies)
        
        print num_bookies,' bookies'
        
        for i in range(0, num_bookies-2):
            bookie = bookies.xpath('aside/a/@title').extract()[i]
            winner_1_odds_i = winner_1_odds.xpath('@data-odig').extract()[i]
            winner_2_odds_i = winner_2_odds.xpath('@data-odig').extract()[i]
            draw_odds_i = draw_odds.xpath('@data-odig').extract()[i]
            
            data_1 = {
                "time": timestamp,
                "match": matchname,
                "bookie": bookie,
                "outcome": winner_1,
                "decodds": winner_1_odds_i
            }
            
            data_2 = {
                "time": timestamp,
                "match": matchname,
                "bookie": bookie,
                "outcome": winner_2,
                "decodds": winner_2_odds_i
            }
            
            data_draw = {
                "time": timestamp,
                "match": matchname,
                "bookie": bookie,
                "outcome": 'draw',
                "decodds": draw_odds_i
            }
            
            tableCheck('match_winner',[])
            
            scraperwiki.sqlite.save(unique_keys=[],table_name='match_winner',
                                    data=data_1)
            scraperwiki.sqlite.save(unique_keys=[],table_name='match_winner',
                                    data=data_2)
            scraperwiki.sqlite.save(unique_keys=[],table_name='match_winner',
                                    data=data_draw)
            
            # print matchname,',',timestamp,',',bookie,',',winner_1,',',winner_1_odds_i
            # print matchname,',',timestamp,',',bookie,',',winner_2,',',winner_2_odds_i
            # print matchname,',',timestamp,',',bookie,',draw,',draw_odds_i
            i+=1
            
            
def tableCheck(table,base=None):
    if base is None: base=[]
    base=base+[ \
            ('time','datetime'), \
            ('match', 'text' ), \
            ('bookie','text'), \
            ('outcome','text'), \
            ('decodds', 'real') 	]
    fields=', '.join([' '.join( map(str,item) ) for item in base ])
    tabledef="CREATE TABLE IF NOT EXISTS '{table}' ( {fields} )".format(table=table,fields=fields)
    scraperwiki.sqlite.execute( tabledef )

process = CrawlerProcess()
process.crawl(OddscheckerSpider)
process.start()