from datetime import datetime
import scrapy
from bs4 import BeautifulSoup
import pandas as pd
import hashlib


class ProjectorsSpider(scrapy.Spider):
    name = "projectors"
    allowed_domains = ['http://www.projectorcentral.com']
    start_urls = ['http://http://www.projectorcentral.com/']

    def start_requests(self):
        urls = [
            'http://www.projectorcentral.com/projectors.cfm?ll=&tt=0&td2=0&c=&g=1&i=d&pxs=0&mfg=&pjh=0&br=&dt=&p=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&c4k=0&wr=&pjw=0&sp=&wt=&st=1&pjw_m=&hide=0&w_m=&sz=15&pjl_m=&is=0&cn=&td2_m=&td=0&ar=&oop=2&lag=0%2C150&tr2=0&dvi=&zr=&sort=pop&ltg=&tr=0&db=',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&td2=0&c=&g=1&i=d&pxs=0&mfg=&pjh=0&dt=&br=&p=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&c4k=0&wr=&pjw=0&sp=&wt=&st=16&pjw_m=&hide=0&w_m=&sz=15&pjl_m=&is=0&cn=&td2_m=&td=0&ar=&oop=2&lag=0%2C150&tr2=0&dvi=&zr=&sort=pop&db=&tr=0&ltg=',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&td2=0&c=&g=1&i=d&pxs=0&mfg=&pjh=0&dt=&br=&p=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&c4k=0&wr=&pjw=0&wt=&sp=&st=31&pjw_m=&hide=0&w_m=&sz=15&is=0&pjl_m=&cn=&td2_m=&td=0&ar=&oop=2&lag=0%2C150&tr2=0&dvi=&zr=&sort=pop&tr=0&db=&ltg=',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&td2=0&c=&g=1&i=d&pxs=0&pjh=0&mfg=&br=&dt=&p=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&c4k=0&wr=&pjw=0&wt=&sp=&pjw_m=&st=46&hide=0&w_m=&sz=15&pjl_m=&is=0&cn=&td2_m=&td=0&oop=2&ar=&lag=0%2C150&tr2=0&dvi=&sort=pop&zr=&db=&tr=0&ltg=',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&td2=0&c=&g=1&i=d&pxs=0&mfg=&pjh=0&br=&dt=&p=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&c4k=0&wr=&pjw=0&wt=&sp=&st=61&pjw_m=&hide=0&w_m=&sz=15&is=0&pjl_m=&cn=&td2_m=&td=0&oop=2&ar=&lag=0%2C150&tr2=0&dvi=&zr=&sort=pop&ltg=&db=&tr=0',
            'http://www.projectorcentral.com/projectors.cfm?ll=&tt=0&td2=0&c=&g=1&i=d&pxs=0&mfg=&pjh=0&p=&br=&dt=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&wr=&c4k=0&pjw=0&wt=&sp=&st=76&pjw_m=&hide=0&w_m=&sz=15&pjl_m=&is=0&cn=&td2_m=&td=0&oop=2&ar=&lag=0%2C150&tr2=0&dvi=&zr=&sort=pop&tr=0&ltg=&db=',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&td2=0&c=&g=1&i=d&pxs=0&pjh=0&mfg=&p=&br=&dt=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&wr=&c4k=0&pjw=0&sp=&wt=&pjw_m=&st=91&hide=0&w_m=&sz=15&pjl_m=&is=0&cn=&td2_m=&td=0&oop=2&ar=&lag=0%2C150&tr2=0&dvi=&sort=pop&zr=&db=&ltg=&tr=0',
            'http://www.projectorcentral.com/projectors.cfm?tt=0&ll=&c=&td2=0&g=1&i=d&pxs=0&mfg=&pjh=0&p=&br=&dt=&r=&pjl=0&t=&pjh_m=&w=&td_m=&is_m=&wr=&c4k=0&pjw=0&wt=&sp=&pjw_m=&st=106&hide=0&w_m=&sz=15&is=0&pjl_m=&cn=&td2_m=&td=0&oop=2&ar=&lag=0%2C150&tr2=0&dvi=&sort=pop&zr=&ltg=&db=&tr=0',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url
        filename = 'data/raw/projectors-{}.csv'.format(hashlib.md5(page.encode('ascii')).hexdigest())
        soup = BeautifulSoup(response.body, 'lxml')
        table = soup.find('table', {'class': 'partList'})

        specs = []
        for proj in table.find_all(['tr']):
            if None not in proj.get_attribute_list('class'):
                if 'upper' in proj['class']:
                    strings = [el.replace('\n', '').replace('\t', '') for el in proj.stripped_strings]
                    data = {'name': strings[0],
                            'release_date': strings[2],
                            'price_dollars': strings[-1],
                           }
                if 'lower' in proj['class']:
                    keys =   [dt.text.replace('\u2009', ' ').replace('\n', '').replace('\t', '').replace('\r', '').strip()[:-1] for dt in proj.find_all('dt')]
                    values = [dd.text.replace('\u2009', ' ').replace('\n', '').replace('\t', '').replace('\r', '').strip() for dd in proj.find_all('dd')]
                    data.update( dict(zip(keys, values)))
                specs.append(data)

        pd.DataFrame(specs[::2]).to_csv(filename, index=False)

        self.log('Saved file %s' % filename)
