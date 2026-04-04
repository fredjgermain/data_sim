import random
import string 

def scramble_letters(text:str, prob:float=0.03): 
  shuffled = list(text) 
  random.shuffle(shuffled) 
  
  scrambling_prob = [random.uniform(0, 1) >= prob for _ in range(len(text))] 
  zipped = zip(list(text), shuffled, scrambling_prob) 
  scrambled = [ a if c else b for a,b,c in zipped ] 
  
  res = []
  for a in scrambled: 
    p = random.uniform(0, 1) 
    if p >= prob: 
      res.append(a) 
    elif p >= prob/2: # ! Add a random letter 
      res.append(random.choice(string.ascii_lowercase)) 
  
  return ''.join(res) 


def missing_elements(elements:list, prob:float=0.03): 
  return [c for c in elements if random.uniform(0,1) >= prob ] 

