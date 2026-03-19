use std::collections::HashSet;
use array_lib::{ArrayDim, DimLabel, DimSize};
use serde::{Deserialize, Serialize};

/// CopyPlanner manages how data is copied from a collection of buffers described by some data layout
/// where each dimension has a meaning (DimSize). The cop_data method assumes you have opened the
/// correct file associated with the object index (use resolve split method to get buffer index).
/// Copy planner will fail to build if dimensions are not compatible. For instance, retrieving
/// sub slices of array dimensions is not supported.
/// A common use-case for this is having multiple bruker fids, each containing more than 1 diffusion
/// encoded volume, or a single fid file containing more than one echo, where only one echo needs to
/// be extracted. This also normalizes the data layout of the object data to make further processing
/// more general. The data layout follows the label conventions introduced in BART.
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct CopyPlanner {
    /// layout of the object
    pub obj_layout:Vec<DimSize>,
    /// layout of the raw data
    pub raw_layout:Vec<Vec<DimSize>>,
    /// dimensional mapping from object space to raw layout space
    dim_mappings: Vec<Vec<[usize;2]>>,
    /// unvisited dimension in raw layout. These dimensions are assumed to contain replicates of the object
    unvisited:Vec<Vec<usize>>,
    /// sizes of unvisited dimensions
    unvisited_sizes:Vec<Vec<usize>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_copy_planner_case1() {
        let obj_dims = vec![DimSize::READ(2),DimSize::PHS1(3)];
        let raw_layout = vec![
            vec![DimSize::COIL(4),DimSize::READ(2),DimSize::PHS1(3)],
        ];
        let cp = CopyPlanner::new(&obj_dims,&raw_layout);
        let src = (0..24).collect::<Vec<usize>>();
        let mut dst = vec![0;6];
        cp.copy_data(0,&src,&mut dst);
        assert_eq!(&dst,&[0, 4, 8, 12, 16, 20]);
    }

    #[test]
    fn test_copy_planner_case2() {
        let obj_dims = vec![DimSize::READ(2),DimSize::PHS1(3)];
        let raw_layout = vec![
            vec![DimSize::READ(2),DimSize::PHS1(3),DimSize::SLICE(10)],
            vec![DimSize::READ(2),DimSize::PHS1(3),DimSize::SLICE(1)],
        ];
        let cp = CopyPlanner::new(&obj_dims,&raw_layout);
        let src = (0..60).collect::<Vec<usize>>();
        let mut dst = vec![0;6];
        cp.copy_data(9,&src,&mut dst);
        assert_eq!(&dst,&[54, 55, 56, 57, 58, 59]);

        let src = (0..6).collect::<Vec<usize>>();
        cp.copy_data(10,&src,&mut dst);
        assert_eq!(&dst,&[0, 1, 2, 3, 4, 5]);
    }
}

impl CopyPlanner {

    /// copies data from src to dst from buffers described by the object and raw data layout
    pub fn copy_data<T:Copy + Sized>(&self, obj_idx:usize, src:&[T], dst:&mut [T]) {

        // return the local and global index for the raw src data
        let (g_idx,idx) = self.raw_indices(obj_idx);

        // retrieve dimension and unvisited dimensions
        let dim_mapping = &self.dim_mappings[g_idx];
        let unvisited = &self.unvisited[g_idx];

        // build array dim for destination space
        let dst_space = self.obj_dims();
        assert_eq!(dst.len(),dst_space.numel());

        // build array dim for source space
        // we cannot use the with_dim syntax because we need to preserve the layout order.
        // we don't care about the object
        let src_space = self.src_dims(obj_idx);

        assert_eq!(src.len(),src_space.numel());

        dst.iter_mut().enumerate().for_each(|(i,z)| {
            let a = dst_space.calc_idx(i);
            let mut b = a.clone();
            // transform b with the dim mappings
            for m in dim_mapping {
                b[m[1]] = a[m[0]];
            }
            // set the unvisited indices based on object index
            unvisited.iter().zip(idx.iter()).for_each(|(&x,&y)|{
                b[x] = y;
            });
            let src_i = src_space.calc_addr(&b);
            *z = src[src_i]
        });

    }

    pub fn obj_dims(&self) -> ArrayDim {
        let mut dst_space = ArrayDim::new();
        self.obj_layout.iter().for_each(|dim| {
            dst_space = dst_space.with_dim_from_label(*dim);
        });
        dst_space
    }

    pub fn src_dims(&self,obj_idx:usize) -> ArrayDim {
        let (g_idx,..) = self.raw_indices(obj_idx);
        let src_shape:Vec<usize> = self.raw_layout[g_idx].iter().map(|dim|dim.size()).collect();
        ArrayDim::from_shape(&src_shape)
    }

    /// infers the number of objects contained in the raw layout
    pub fn n_objects(&self) -> usize {
        self.object_split().iter().sum()
    }

    /// returns the object split over multiple source buffers. The sum of these values is equal to
    /// the total number of objects
    fn object_split(&self) -> Vec<usize> {
        self.unvisited_sizes.iter().map(|s|s.iter().product::<usize>()).collect()
    }

    /// returns the group index from the object index for cases where multiple buffers and
    /// data layouts are used for the src data. This informs which buffer to read
    pub fn group_index(&self,obj_index:usize) -> usize {
        let (group_idx,..) = self.resolve_split(obj_index);
        group_idx
    }

    /// converts an object index to raw layout indices for address calculations
    pub fn raw_indices(&self,obj_index:usize) -> (usize,Vec<usize>) {
        let (group_idx,local_idx) = self.resolve_split(obj_index);
        let a = ArrayDim::from_shape(&self.unvisited_sizes[group_idx]);
        let n = a.shape_squeeze().len();
        (group_idx,a.calc_idx(local_idx)[0..n].to_vec())
    }

    /// resolves the group index and the local index from the obj index
    fn resolve_split(&self,obj_idx:usize) -> (usize,usize) {
        assert!(obj_idx < self.n_objects(), "object index is out of bounds");
        let mut offset = 0;
        for (group_idx, &size) in self.object_split().iter().enumerate() {
            if obj_idx < offset + size {
                let local_idx = obj_idx - offset;
                return (group_idx, local_idx);
            }
            offset += size;
        }
        panic!("failed to resolve split");
    }

    /// build a new copy planer from pre-defined data layouts
    pub fn new(obj_layout:&[DimSize],raw_layout:&[Vec<DimSize>]) -> CopyPlanner {

        let mut all_mappings = vec![];
        let mut all_unvisited = vec![];
        let mut all_unvisited_sizes = vec![];

        for buf in raw_layout {

            let mut dim_mappings = vec![];
            let mut not_visited = HashSet::<usize>::from_iter(0..buf.len());

            obj_layout.iter().for_each(|o_dim| {
                let ol:DimLabel = o_dim.into();
                for (i,r_dim) in buf.iter().enumerate() {
                    let rl:DimLabel = r_dim.into();
                    if rl as usize == ol as usize {
                        // get divisor
                        if r_dim.size() != o_dim.size() {
                            panic!("object dimension are incompatible with raw layout: {:?} -> {:?}", o_dim, r_dim);
                        }
                        let mapping = [ol as usize, i];
                        dim_mappings.push(mapping);
                        not_visited.remove(&i);
                        break
                    }
                }
            });

            let mut unvisited:Vec<usize> = not_visited.into_iter().collect();
            unvisited.sort();
            let unvisited_sizes:Vec<usize> = unvisited.iter().map(|i|buf[*i].size()).collect();

            all_mappings.push(dim_mappings);
            all_unvisited.push(unvisited);
            all_unvisited_sizes.push(unvisited_sizes);

        }

        CopyPlanner {
            obj_layout: obj_layout.to_vec(),
            raw_layout: raw_layout.to_vec(),
            dim_mappings: all_mappings,
            unvisited: all_unvisited,
            unvisited_sizes: all_unvisited_sizes,
        }

    }
}