 
import fileinput

def parseFile(xmlFile):
  count = 0
  currentArticle = 1
  temp = ""
  inPage = False
  for line in fileinput.input([xmlFile]):
    if "<page>" in line:
      inPage = True
    
    if "</page>" in line:
      inPage = False
      temp += line
      f = open('articles/'+str(currentArticle),'w')
      f.write(temp)
      f.close()
      temp = ""
      currentArticle = currentArticle + 1
      count = count + 1
      if (count % 100) == 0 :
	print count
    
    if inPage:
      temp += line
      
    