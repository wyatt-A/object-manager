use array_lib::{ArrayDim, DimLabel, DimSize};
use array_lib::DimSize::SLICE;
use object_manager::ObjectManagerConf;

fn main() {

    let conf = ObjectManagerConf::default();

    println!("{}",conf.to_json());

    // array object to fill: [512,8192,4]
    let obj_dims =  ArrayDim::new()
        .with_dim_from_label(DimSize::READ(512))
        .with_dim_from_label(DimSize::PHS1(8192))
        .with_dim_from_label(DimSize::TE(4));

    // larger data buffer: [512,8,8192]
    let raw_layout = vec![DimSize::READ(512),DimSize::TE(8),DimSize::PHS1(8192)];

    // I want to implicitly read the first 4 'TEs' to fill the object based on some index, in this case (0 or 1)
    // is there a general solution to this using array strides and mem copies?
}
