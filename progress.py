from tqdm import tqdm
import math

# A function to indicate process
class Progress:
  def __init__(self, total, min_batches=1):
    self.progress = 0
    self.current_batch = 0
    self.total = total
    self.min_batches = min_batches # Process data in batches to avoid crashings
    self.items_per_batch = math.floor(float(total) / float(min_batches))
    self.batches = math.ceil(float(total) / float(self.items_per_batch))
    print('Total Items:', self.total)
    print('Items Per Batch:', self.items_per_batch)
    print('Actual Batches:', self.batches)
    self.pbar = tqdm(total=total, desc='Total Items')
    self.bbar = tqdm(total=self.batches, desc='Current Batches')

  def count(self):
    self.progress += 1
    self.pbar.update(1)
    if self.finished():
      self.current_batch += 1 # Create the last batch
      self.bbar.update(1)
      self.pbar.close()
      self.bbar.close()
    elif self.progress % self.items_per_batch == 0:
      self.current_batch += 1
      self.bbar.update(1)

  def finished(self):
    return self.progress >= self.total

  def is_batch_complete(self):
    return (self.current_batch == self.batches 
      or self.progress % self.items_per_batch == 0)